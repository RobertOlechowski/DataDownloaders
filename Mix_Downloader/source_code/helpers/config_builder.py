from ROTools.Config.ConfigLoader import build_config

def load_config(skip_dump=True):
    return build_config(file_name='config/config.yaml', skip_dump=skip_dump, prefix="MIX_")