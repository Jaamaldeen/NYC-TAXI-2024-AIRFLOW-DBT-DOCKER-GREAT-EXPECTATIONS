import os
import requests



base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

for month in range(1, 13):
    file_name = f"yellow_tripdata_2024-{month:02d}.parquet"
    url = base_url + file_name
    save_path = os.path.join("data", file_name)

    print(f"⬇️ Downloading {file_name} ...")

    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Saved: {save_path}")
    else:
        print(f"Failed to download {url} (Status: {response.status_code})")