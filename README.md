# Multiverse: Extendable Autoscaling Policies Simulation across Resource Abstraction Layers

Ideas for experimentation:
- adaptable scaling policy computation interval
- considering structure of the app for scaling


Structure-awareness ideas/links:
- https://towardsdatascience.com/four-ways-to-quantify-synchrony-between-time-series-data-b99136c4a9c9
- https://towardsdatascience.com/time-series-smoothing-for-better-forecasting-7fbf10428b2

also forecasting:
ssa: https://www.kaggle.com/jdarcy/introducing-ssa-for-time-series-decomposition
arima: https://towardsdatascience.com/machine-learning-part-19-time-series-and-autoregressive-integrated-moving-average-model-arima-c1005347b0d7

Check some warnings:
WARNING:tensorflow:5 out of the last 5 calls to <function Model.make_train_function.<locals>.train_function at 0x00000257CE84BD90> triggered tf.function retracing. Tracing is expensive and the excessive number of tracings could be due to (1) creating @tf.function repeatedly in a loop, (2) passing tensors with different shapes, (3) passing Python objects instead of tensors. For (1), please define your @tf.function outside of the loop. For (2), @tf.function has experimental_relax_shapes=True option that relaxes argument shapes that can avoid unnecessary retracing. For (3), please refer to https://www.tensorflow.org/guide/function#controlling_retracing and https://www.tensorflow.org/api_docs/python/tf/function for  more details.
