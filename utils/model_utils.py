import tensorflow as tf
import pickle
import os
import numpy as np

from ml_model.gen_model import GenModel

os.environ["CUDA_DEVICE_ORDER"]="PCI_BUS_ID"
os.environ["CUDA_VISIBLE_DEVICES"]="1"

physical_devices = tf.config.list_physical_devices('GPU')
for physical_device in physical_devices:
    tf.config.experimental.set_memory_growth(physical_device, True)

tfk = tf.keras

def load_models(visible_dist):
    
    # scaler

    with open('./ml_model/saved_models/scaler.pickle', 'rb') as f:
        scaler = pickle.load(f)

    # neural network model

    dist = GenModel((8,), (15,), distribution=visible_dist, num_mixtures=20)
    inp = tfk.Input(shape=(8,))
    out = dist.network.call(inp)

    model = tfk.Model(inputs=inp, outputs=out)

    save_dir = './ml_model/saved_models/window_logic_gen_model_' + visible_dist + '/'

    model.load_weights(save_dir + 'weights.hdf5')

    return scaler, dist

def ensure_increasing_order(arr):
    for i in range(1, len(arr)):
        if arr[i] < arr[i - 1]:
            arr[i] = arr[i - 1]
    return arr

def get_window(x, scaler, dist, agg_type='median'):

    x = np.array(x).reshape(1, -1)
    x = scaler.transform(x)
    pred = np.array(dist.sample_n(x, 10000))

    if agg_type == 'median':
        pred = np.squeeze(np.round(np.median(pred, axis=0)).astype('int32'))
    elif agg_type == 'mean':
        pred = np.squeeze(np.round(np.mean(pred, axis=0)).astype('int32'))

    pred[pred >= 500] = 5000

    ensure_increasing_order(pred)
    
    try:
        index = np.where(pred == 5000)[0][0]
        pred = pred[:index + 1]
    except:
        pred = np.append(pred, 5000)
    
    keys = (np.arange(len(pred))*1000).astype('int32')

    return dict(zip(keys.astype('str'), pred.astype('str')))