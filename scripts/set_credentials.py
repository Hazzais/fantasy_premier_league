import getpass
import keyring

if __name__ == '__main__':
    print("You are now setting credentials")

    # Maybe too general - always using fpl database at the moment
    service_or_db = input("Please enter a name/id for this service or"
                          "database: ")
    user = input("Please enter a user for this service or database: ")
    password = getpass.getpass("Please enter the password for this user:")
    keyring.set_password(f'{service_or_db}',
                         user,
                         password)
