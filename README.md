# Scrabu

Data-Driven Optimization of the Revisiting Policy for a Domain-Specific Crawler with Integrated Shipment Status Scraper

## Installation

The project is dockerized. Use the following Makefile target to build the Docker image with all required dependencies.
Dependencies are all defined in the requirements.txt file. In case you change them, you should re-run the build command
to update the dependencies.

```bash
make build
```

## Usage

To start the Jupyter notebook server use the following command:

```bash
make up
```

## License
[MIT](https://choosealicense.com/licenses/mit/)
