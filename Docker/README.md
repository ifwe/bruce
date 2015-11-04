# Bruce Dockerfile


## Running Bruce as Docker

To run bruce as docker all you need to do is:
 
 1. Build the docker image using the dockerfile attached
 2. Run the docker with shared volume to other dockers
 3. Launch your Application's docker (the ones using Bruce) with shared volume to the socket file

### build the dockerfile

``` docker build -t <host you docker repo>/bruce:latest . ```

### Run the docker with shared volume

``` docker run -d --name bruce -p 9090:9090 -v /root/ <host you docker repo>/bruce ```

sharing the /root volume will enable other dockers sending messages to bruce to use this shared volume for the socket file

### Launch your Application's docker with shared volume to the socket file

``` docker run -d --name myapp --volumes-from bruce -e BRUCE_SOCKET=/root/bruce.socket <host you docker repo>/myapp:latest ```



