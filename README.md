# Overview

Produces files from Movie Lens data with:

 - (CSV) Number of movies rated & average rating by user
 - (CSV) Number of movies by genre
 - (Parquet) Top 100 movies

# Project Setup

You need `sbt` installed to build and run tests.

Mac (assumes brew is installed): `brew install sbt`

Debian distros:

```
echo "deb http://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-get update
sudo apt-get install -y --force-yes sbt
```

Yum distros:

```
curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
sudo yum install sbt
```

## Bootstrap - Download & Unzip Movie Lens Data

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
