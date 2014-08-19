## Setting Up the Google Test Framework

To set up the Google Test Framework, execute the following commands:

```
wget http://googletest.googlecode.com/files/gtest-1.7.0.zip
unzip gtest-1.7.0.zip
cd gtest-1.7.0
./configure
make
cd lib/.libs
mv *.a *.lai *.so* ../*.la /usr/lib64
ldconfig
cd ../..
cp -a include/gtest /usr/include
```

Now [continue setting up your build environment](https://github.com/tagged/bruce/blob/master/README.md#setting-up-a-build-environment).
