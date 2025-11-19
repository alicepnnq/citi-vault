import requests, zipfile, io

def extract_citibike_data(year=2024, month=1):
    filename = "202401-citibike-tripdata.zip"
    url = f"https://s3.amazonaws.com/tripdata/{filename}"

    print(f"Downloading: {url}")
    response = requests.get(url, allow_redirects=True)
    print("Final URL:", response.url)

    if response.status_code == 200:
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall("data/raw")
        print("✅ Extracted", filename)
    else:
        print("❌ Failed:", response.status_code)

if __name__ == "__main__":
    extract_citibike_data(2024, 1)
