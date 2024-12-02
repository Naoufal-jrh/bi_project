from cryptography.fernet import Fernet
print("key    --- >      "+Fernet.generate_key().decode())