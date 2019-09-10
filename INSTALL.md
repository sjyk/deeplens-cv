==Installing the DeepLens Storage Manager==
These steps will walk you through setting up and installing the DeepLens Storage Manager. The initial instructions will focus on the basic storage engine that uses the filesystem to store video files.

===Environment===
DeepLens has a Python 3 programming interface. Make sure that version of python that you are using is Python 3.5+
```
$ python -V
Python 3.7.0
```
Once you have confirmed that your programming environment is sufficient, you can download the package with:
```
$ git clone https://github.com/sjyk/deeplens-storage.git
$ cd deeplens-storage
```

===Install ffmpeg===
DeepLens relies on ffmpeg to transcode videos. You can install ffmpeg on Ubuntu Linux with:
```
$ sudo apt install ffmpeg
```
On MacOS with homebrew:
```
$ brew install ffmpeg
```

===Install Python Requirements===
The python requirements can be installed by running:
```
$ pip install -r requirements.txt
```

===Testing the Setup===
To test the setup, you can run our benchmark suite. This will run several tests using clips found on the internet. You can compare these results to those found in the benchmark folder to identify performance differences.
```
$ python install_test.py > benchmarks/mysetup.json
```
