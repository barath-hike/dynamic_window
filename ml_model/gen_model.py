import tensorflow as tf
import tensorflow_probability as tfp
from tensorflow_probability.python.distributions import distribution

tfk = tf.keras
tfkl = tf.keras.layers
tfd = tfp.distributions
tfpl = tfp.layers

class GenModel(distribution.Distribution):

    def __init__(self, input_shape, output_shape, distribution='gaussian', num_mixtures=20):

        self.input_shape = input_shape
        self.output_shape = output_shape
        self.num_mixures = num_mixtures
        self.distribution = distribution

        if self.distribution in ['gaussian', 'lognormal']:
            self.output_length = output_shape[0] * 2
        elif self.distribution in ['mixture_of_gaussians', 'mixture_of_lognormal']:
            self.output_length = output_shape[0] * 3 * self.num_mixures

        self.network = tfk.Sequential([
            tfkl.InputLayer(input_shape=input_shape),
            tfkl.Dense(32),
            tfkl.BatchNormalization(),
            tfkl.ELU(),
            tfkl.Dense(64),
            tfkl.BatchNormalization(),
            tfkl.ELU(),
            tfkl.Dense(128),
            tfkl.BatchNormalization(),
            tfkl.ELU(),
            tfkl.Dense(256),
            tfkl.BatchNormalization(),
            tfkl.ELU(),
            tfkl.Dense(512),
            tfkl.BatchNormalization(),
            tfkl.ELU(),
            tfkl.Dropout(0.2),
            tfkl.Dense(1024),
            tfkl.BatchNormalization(),
            tfkl.ELU(),
            tfkl.Dropout(0.2),
            tfkl.Dense(self.output_length)
        ])

        self.network.build(input_shape)

    def mixture_dist(self, params):

        if self.distribution == 'gaussian':

            loc_params, scale_params = tf.split(params, 2, axis=-1)

            loc_params = tf.math.sigmoid(tf.reshape(loc_params, (-1, self.output_shape[0]))) * 500
            scale_params = tf.math.softplus(tf.reshape(scale_params, (-1, self.output_shape[0])))

            distribution = tfd.Independent(tfd.Normal(loc=loc_params, scale=scale_params), reinterpreted_batch_ndims=0)

        elif self.distribution == 'mixture_of_gaussians':

            loc_params, scale_params, component_params = tf.split(params, 3, axis=-1)

            loc_params = tf.math.sigmoid(tf.reshape(loc_params, (-1, self.output_shape[0], self.num_mixures))) * 500
            scale_params = tf.math.softplus(tf.reshape(scale_params, (-1, self.output_shape[0], self.num_mixures)))
            component_params = tf.math.softmax(tf.reshape(component_params, (-1, self.output_shape[0], self.num_mixures)))

            mixtures = tfd.MixtureSameFamily(
                mixture_distribution=tfd.Categorical(probs=component_params),
                components_distribution=tfd.Normal(loc=loc_params, scale=scale_params)
            )

            distribution = tfd.Independent(mixtures, reinterpreted_batch_ndims=0)

        elif self.distribution == 'lognormal':

            loc_params, scale_params = tf.split(params, 2, axis=-1)

            loc_params = tf.math.sigmoid(tf.reshape(loc_params, (-1, self.output_shape[0]))) * 500
            scale_params = tf.math.softplus(tf.reshape(scale_params, (-1, self.output_shape[0])))

            distribution = tfd.Independent(tfd.LogNormal(loc=loc_params, scale=scale_params), reinterpreted_batch_ndims=0)

        elif self.distribution == 'mixture_of_lognormal':

            loc_params, scale_params, component_params = tf.split(params, 3, axis=-1)

            loc_params = tf.math.sigmoid(tf.reshape(loc_params, (-1, self.output_shape[0], self.num_mixures))) * 500
            scale_params = tf.math.softplus(tf.reshape(scale_params, (-1, self.output_shape[0], self.num_mixures)))
            component_params = tf.math.softmax(tf.reshape(component_params, (-1, self.output_shape[0], self.num_mixures)))

            mixtures = tfd.MixtureSameFamily(
                mixture_distribution=tfd.Categorical(probs=component_params),
                components_distribution=tfd.LogNormal(loc=loc_params, scale=scale_params)
            )

            distribution = tfd.Independent(mixtures, reinterpreted_batch_ndims=0)

        return distribution

    def loss(self, y_true, y_pred):

        dist = self.mixture_dist(y_pred)
        nll = -dist.log_prob(y_true)

        nll = tf.where(nll < 0, tf.ones_like(nll) * 1e-6, nll)

        return tf.reduce_mean(nll)

    def sample_n(self, inputs, n):

        params = self.network(inputs)

        dist = self.mixture_dist(params)

        return dist.sample(n)