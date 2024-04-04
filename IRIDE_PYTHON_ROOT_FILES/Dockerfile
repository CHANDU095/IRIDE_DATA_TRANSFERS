FROM python:3.11

WORKDIR /cont_workspace

#sudo chown -R $(whoami) /home/chandu/project_prisma_dr/codes_wsl/

COPY requirements.txt requirements.txt

RUN apt-get update -y && \
    apt-get install -y python3-tk qtbase5-dev tree pandoc libgl1-mesa-glx gdal-bin libgdal-dev&& \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \ 
    pip install --user --force-reinstall "GDAL[numpy]==3.6.2" && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Create .cdsapirc file
RUN echo "url: https://cds.climate.copernicus.eu/api/v2\n key: 275818:767ac954-78f6-4ad6-a38b-4a444a1c82b9" > /root/.cdsapirc


EXPOSE 8888 
EXPOSE 6006

ENTRYPOINT ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--no-browser"]


#ogrinfo --version
#pip install GDAL==3.6.2
#pip install --user --force-reinstall "GDAL[numpy]==3.6.2"
