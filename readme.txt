python3 -m venv prefect-env
source prefect-env/bin/activate
prefect server start
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

#Membuat workpool jika belum ada
prefect work-pool create -t process my-etl

#Menjalankan worker 
prefect worker start -p my-etl

##Deployment
prefect init 
-edit prefect.yaml
prefect deploy

##Jika ada update pada Deployment lakukan
prefect deploy dan prefect deployment run

#Jika perlu config file maka harus di export
1. pada prefect.yaml
2. pada script .py nya (sample ada pada sample_etl_with_env.py)
