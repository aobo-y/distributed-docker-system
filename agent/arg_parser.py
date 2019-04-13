import argparse

def get_parser():
    parser = argparse.ArgumentParser(description='agent parameters')
    # master url
    parser.add_argument('--master_url', default='', type=str)
    return parser