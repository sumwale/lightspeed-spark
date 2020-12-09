# Add the default user/group

SPARK_USER=spark
SPARK_GROUP=spark
SPARK_HOME=/opt/$SPARK_USER

[ "$DPKG_MAINTSCRIPT_PACKAGE" ] && quiet="--quiet"

if ! getent group $SPARK_GROUP > /dev/null; then
   addgroup --system $quiet $SPARK_GROUP
fi
if ! getent passwd $SPARK_USER > /dev/null; then
  adduser --system $quiet --home $SPARK_HOME --no-create-home --shell /bin/bash \
      --ingroup $SPARK_GROUP --gecos "LightSpeed Spark cluster owner" $SPARK_USER
fi
