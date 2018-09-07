FROM quay.octanner.io/base/oct-scala:2.12.2-sbt-0.13.15-play-2.6.1

COPY build.sbt start.sh ./
RUN chmod +x ./start.sh
COPY project project/
RUN sbt update

## If you copy the source after the update, then you can actually cache artifacts
COPY conf conf
COPY app app

RUN sbt compile stage

ENTRYPOINT ["./start.sh"]
