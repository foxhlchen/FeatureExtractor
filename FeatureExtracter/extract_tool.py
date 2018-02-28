from keras.preprocessing import image
import keras
import numpy as np
import struct


class FeatureExtractor(object):

    def __init__(self):
        self.model_ns = keras.applications.resnet50
        self.model = self.model_ns.ResNet50(weights='imagenet', include_top=False)

    def extract(self, img_path: str):
        img = image.load_img(img_path, target_size=(224, 224))
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        x = self.model_ns.preprocess_input(x)

        features = self.model.predict(x)
        return features.flatten()

    def extract_bytes(self, img_path: str) -> bytes:
        return self.pack(self.extract(img_path).tolist())

    @staticmethod
    def pack(feature: list) -> bytes:
        feature_bytes = struct.pack('f' * 2048, *feature)
        return feature_bytes

    @staticmethod
    def unpack(feature_bytes: bytes) -> list:
        f_restore = struct.unpack('f' * 2048, feature_bytes)
        return list(f_restore)
