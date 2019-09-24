from utils import print_and_log, get_config_field

def google_auth():
    email = get_config_field('GOOGLEAUTH', 'CLIENT_EMAIL')
    api_key = get_config_field('GOOGLEAUTH', 'PRIVATE_KEY')
    print_and_log('email', email)
    print_and_log('api_key', api_key)
