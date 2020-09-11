echo "Downloading 2GB of input data."
CARCLOUD="beijing_allIn1day_ts_sorted.txt"
LR="h1.txt"
CARLOCAL="carLocalInput.zip"
echo "Downloading input data to data/input/"
echo
echo "File 1/3"
wget -O data/input/${CARCLOUD} https://chalmersuniversity.box.com/shared/static/qzqvlsatyb37a9d3kvfk224ehj0bipki.txt
echo
echo "File 2/3"
wget -O data/input/${CARLOCAL} https://chalmersuniversity.box.com/shared/static/7s9ewtys69aik5p8bwapbazjs2u9l8vv.zip
echo
echo "File 3/3"
wget -O data/input/${LR} https://chalmersuniversity.box.com/shared/static/ioal17insfry4naurtybkp44dxev59ta.txt
echo
echo "Extracting files..."
unzip -q data/input/${CARLOCAL} -d data/input/
rm data/input/${CARLOCAL}
echo "Input data downloaded to data/input"
