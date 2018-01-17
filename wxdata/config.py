import os

ROOT_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


def get_resource(filepath):
    return os.path.join(ROOT_PROJECT_DIR, 'resources', filepath)