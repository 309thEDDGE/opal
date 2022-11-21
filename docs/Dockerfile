FROM registry.il2.dso.mil/skicamp/project-opal/tip:faf305cd

SHELL ["/bin/bash", "-c"]
COPY --chown=user . /mnt

ENTRYPOINT [ "/usr/bin/bash"  ]

RUN conda init bash && source ~/.bashrc && conda activate singleuser && conda install yaml pyyaml pytest -y
CMD [ "-c", "cd /mnt && source ~/.bashrc && conda activate singleuser && pytest -vvv -p no:cacheprovider "] 





