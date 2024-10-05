import logging
import logging.config

# Create a logger
appLogger = logging.getLogger('appLogger')
appLogger.setLevel(logging.DEBUG)  # Set the logging level

# Create handlers
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler('app.log')

# Set the logging level for handlers
console_handler.setLevel(logging.DEBUG)
file_handler.setLevel(logging.DEBUG)

# Create formatters
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add formatters to handlers
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handlers to the logger
appLogger.addHandler(console_handler)
appLogger.addHandler(file_handler)


