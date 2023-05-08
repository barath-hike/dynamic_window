import numpy as np
import pickle

time_threshold = 11
zeros_threshold = 0.15

population_size = 1000

values_to_use = np.concatenate((np.arange(1,10), np.arange(10,50,5), np.arange(50,150,25), np.arange(150,501,50)))

with open('./ml_model/saved_models/game_encoding.pickle', 'rb') as f:
    game_encoding = pickle.load(f)

with open('./ml_model/saved_models/lgbm_time_zeros_pred.pickle', 'rb') as f:
    reg_1, reg_2 = pickle.load(f)

def mating_selection_pool(features, population, num_parents=20, best=False):

    feats = np.concatenate((
        np.tile(features, (population.shape[0], 1)), population
    ), axis=-1)

    y_pred_1 = reg_1.predict(feats)
    y_pred_2 = reg_2.predict(feats)

    y_error_1 = y_pred_1 / time_threshold
    y_error_2 = y_pred_2 / zeros_threshold

    y_error = np.abs((y_error_1 + y_error_2) / 2 - 1)

    if not best:

        indices = np.argpartition(y_error, num_parents)[:num_parents]

        return population[indices], y_error[indices]

    else:

        index = np.argmin(y_error)

        return population[index], y_error[index]

def crossover(parents, offspring_size=population_size):

    offsprings = []

    for i in range(offspring_size):

        parents_selected = parents[np.random.randint(low=0, high=len(parents), size=(2,))]

        random_mask = np.random.uniform(size=parents.shape[1]) < 0.5
        offspring = np.where(random_mask, parents_selected[0, :], parents_selected[1, :])

        offsprings.append(offspring)

    offsprings = np.array(offsprings)

    return offsprings

def mutation(offsprings, mutation_rate=0.05):

    random_mask = np.random.uniform(size=offsprings.shape) < mutation_rate
    offsprings = np.where(random_mask, np.random.choice(values_to_use, size=offsprings.shape), offsprings)

    return offsprings

def genetic_algorithm(features, num_generations=1000, mutation_rate=0.10, decay_rate=0.99):

    # population = np.random.randint(low=0, high=501, size=(population_size, 15))
    population = np.random.choice(values_to_use, size=(population_size, 15))
    population = np.sort(population, axis=1)

    for generation in range(num_generations):

        parents, error = mating_selection_pool(features, population)
        offsprings = crossover(parents)
        offsprings = mutation(offsprings, mutation_rate=mutation_rate)

        population = offsprings.copy()

        population = np.sort(population, axis=1)

        mutation_rate *= decay_rate

    best_output, error = mating_selection_pool(features, population, best=True)

    return best_output

def ensure_increasing_order(arr):

    for i in range(1, len(arr)):
        if arr[i] < arr[i - 1]:
            arr[i] = arr[i - 1]
    return arr

def window_format(x):

    x = x.astype('int32')
    x[x >= 500] = 5000

    x = ensure_increasing_order(x)

    try:
        index = np.where(x == 5000)[0][0]
        x = x[:index + 1]
    except:
        x = np.append(x, 5000)
    
    keys = (np.arange(len(x))*1000).astype('int32')

    return dict(zip(keys.astype('str'), x.astype('str')))

def get_window(num_users, mm_started, minute, game, table=1):

    game_enc = game_encoding[game]

    features = np.array([num_users, mm_started, minute, game_enc, table]).astype('float32')

    window = genetic_algorithm(features)

    formatted_window = window_format(window)

    return formatted_window