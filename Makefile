# Makefile

env:
	@if [ -d "./ukraine_env" ]; then \
		echo "Environment './ukraine_env' already exists. Activate it with: conda activate ./ukraine_env"; \
	else \
		echo "Environment './ukraine_env' does not exist. Creating it..."; \
		conda create -p ./ukraine_env python=3.12 geopandas gdal -c conda-forge --strict-channel-priority --yes; \
		echo "Installing additional packages with pip..."; \
		./ukraine_env/bin/python -m pip install uv; \
		./ukraine_env/bin/python -m uv pip install -r requirements.txt; \
		echo "Setup complete. Activate it with: conda activate ./ukraine_env"; \
	fi


clean:
	black src
	isort src
	flake8 src