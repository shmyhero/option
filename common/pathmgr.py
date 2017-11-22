import os.path
from utils.iohelper import ensure_dir_exists


class PathMgr(object):

    def __init__(self):
        pass

    @staticmethod
    def get_project_path():
        path_dir = os.path.dirname(os.path.abspath(__file__))
        return path_dir[:path_dir.rindex(os.path.sep)]

    @staticmethod
    def get_log_path(sub_path = None):
        project_path = PathMgr.get_project_path()
        if sub_path:
            log_path = os.path.join(project_path, "logs", sub_path)
        else:
            log_path = os.path.join(project_path, "logs")
        ensure_dir_exists(log_path)
        return log_path

    @staticmethod
    def get_data_path(sub_path = None):
        project_path = PathMgr.get_project_path()
        if sub_path:
            data_path = os.path.join(project_path, "data", sub_path)
        else:
            data_path = os.path.join(project_path, "data")
        return data_path

    @staticmethod
    def get_raw_data_path(sub_path = None):
        raw_path = PathMgr.get_data_path('raw')
        if sub_path:
            data_path = os.path.join(raw_path, sub_path)
        else:
            return raw_path
        return data_path
