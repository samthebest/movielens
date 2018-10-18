# Overview

Produces files from MovieLens data with:

 - (CSV) Number of movies rated & average rating by user
 - (CSV) Number of movies by genre
 - (Parquet) Top 100 movies

# Project Setup

You need `sbt` installed to build and run tests.

Mac (assumes brew is installed): `brew install sbt`

Debian distros:

```
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get install sbt
```

Yum distros:

```
curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
sudo yum install sbt
```

## Bootstrap - Download & Unzip MovieLens Data

If on Mac run `./bin/bootstrap.sh`

If you are on linux please first install `unzip` as it doesn't come with every distro.

e.g. Debian distros: `sudo apt-get install unzip`
e.g. Yum distros: `sudo yum install unzip`

# Build

`sbt assembly`

# Run

After building

`java -cp target/movielens-1.jar`

Output will be in `data/output/`

# Test

All tests `sbt test it:test`. Unit only `sbt test`, end to end only `sbt it:test`.

# Comments

Since

> Each user has at least 20 ratings

No need to join users for 2.A
