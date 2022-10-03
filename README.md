# Real-time pipeline for several NLP modules 

## Instructions on running the pipeline

Pre-requisites:
- *docker* and *psql* should be installed in the system
- all shell scripts should be made executable (`chmod +x filename.sh`)

Initial set up:
- Create a virtual environment named *env* and install dependencies:
    - `python3 -m venv env`
    - `pip install -r requirements.txt`
- Activate the virtual environment: `source env/bin/activate`
- Run `./up.sh` to start containers, set up tables and migrate data

Running the pipeline: 
- Open 3 tabs, activate a virtual environment in each
- In each tab run start pipeline modules in order:
    1) `python3 poster.py` to start a posting module
    2) `./run_nlps.sh` to start nlp modules
    3) `./run_producers.sh` to trigger the pipeline

Wraping up:
- Run `./down.sh` to stop and remove containers

## Extra
To extend the pipeline, use templates from `extend_pipeline` directory and modify `poster.py` following XXX-tags.