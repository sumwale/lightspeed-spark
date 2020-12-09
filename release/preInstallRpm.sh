# Add the default user/group

SPARK_USER=spark
SPARK_GROUP=spark
SPARK_HOME=/opt/$SPARK_USER

if ! getent group $SPARK_GROUP > /dev/null; then
  groupadd -r $SPARK_GROUP
fi
if ! getent passwd $SPARK_USER > /dev/null; then
  useradd -r -M -d $SPARK_HOME -s /bin/bash -N -g $SPARK_GROUP \
      -c "LightSpeed Spark cluster owner" $SPARK_USER
fi
