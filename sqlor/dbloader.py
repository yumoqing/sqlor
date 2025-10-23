from appPublic.worker import get_event_loop

async def main():
	print('to be programming')

if __name__ == '__main__':
	get_event_loop().run_until_complete(main())
