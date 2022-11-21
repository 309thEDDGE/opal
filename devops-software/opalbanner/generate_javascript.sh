cookiecutter https://github.com/jupyterlab/extension-cookiecutter-ts

yarn add -D @jupyterlab/application @lumino/widgets --legacy-peer-deps

pip install -e .

pip install jupyter-packaging

jupyter labextension develop . --overwrite

jlpm run build

jupyter lab --NotebookApp.use_redirect_file=False
