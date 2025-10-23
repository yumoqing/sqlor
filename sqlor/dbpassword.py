import os, sys
from appPublic.aes import aes_encode_b64
from appPublic.jsonConfig import getConfig

def main():
	if len(sys.argv) < 3:
		print(f'{sys.argv[0]} server_path dbuser_password')
		sys.exit(1)
	runpath = sys.argv[1]
	password = sys.argv[2]
	config = getConfig(runpath)
	cyber = aes_encode_b64(config.password_key, sys.argv[2])
	print(f'{password} encoded is {cyber}')
	sys.exit(0)

if __name__ == '__main__':
	main()
