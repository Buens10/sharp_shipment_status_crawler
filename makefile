
.PHONY: start_jupyter

target: start the docker-container of the jupyter_notebook

start_jupyter:
	
	docker start -a 9f571fa86dc2