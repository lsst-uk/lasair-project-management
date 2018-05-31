alert_stream
============

Code to access the ZTF alert stream remotely.

Requires Docker for the usage instructions below.

Usage (single host)
-------------------

Clone repo, cd into directory, and checkout appropriate branch.

**Build docker image**

From the alert_stream directory:

```
$ docker build -t "ztf-listener" .
```

This should now work:

```
$ docker run -it --rm ztf-listener python bin/printStream.py -h
```

You must rebuild your image every time you modify any of the code.

**Consume alert stream**

To start a consumer for printing all alerts from the host "host-name" in the stream "test-stream" to screen:

```
$ docker run -it --rm \
      --network=host \
      --name=$(whoami)_printer \
      ztf-listener python bin/printStream.py host-name test-stream
```

By default, `printStream.py` will not collect postage stamp cutouts.
To enable postage stamp collection, specify a directory to which files should be written with the optional flag `--stampDir <directory name>`.
If run using a Docker container, the stamps and other files written out will be collected within the container.

To collect postage stamp cutouts and output files locally, you can mount a local directory and give the Docker container write access with, e.g., the following command:

```
$ docker run -it --rm \
      --network=host \
      --name=$(whoami)_printer \
      -v {local path to write stamps}:/home/alert_stream/stamps:rw \
      ztf-listener python bin/printStream.py host-name test-stream --stampDir stamps
```

**Shut down and clean up**

Find your containers with `docker ps` and shut down with `docker kill [id]`.
