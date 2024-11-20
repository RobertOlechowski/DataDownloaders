from ROTools.Config.ConfigLoader import build_config

def load_config():
    return build_config(file_name='config/config.yaml', skip_dump=False, prefix="MIX_")