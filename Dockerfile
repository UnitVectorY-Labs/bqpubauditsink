# Stage 1: Build the application
FROM maven:3-amazoncorretto-25 AS build

# Build argument for version injection
ARG VERSION=dev

WORKDIR /app

# Copy your project files
COPY . .

# Build the application
RUN mvn clean package -DskipTests -ntp && \
  rm -rf /app/target/*-javadoc.jar && \
  mkdir -p /app/build && \
  mv /app/target/*.jar /app/build/

# Stage 2: Run the application
FROM gcr.io/distroless/java17-debian13
WORKDIR /app
COPY --from=build /app/build/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
