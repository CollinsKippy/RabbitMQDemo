FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["RabbitMQDemo.Background/RabbitMQDemo.Background.csproj", "RabbitMQDemo.Background/"]
COPY ["RabbitMQDemo.Models/RabbitMQDemo.Models.csproj", "RabbitMQDemo.Models/"]
RUN dotnet restore "RabbitMQDemo.Background/RabbitMQDemo.Background.csproj"
COPY . .
WORKDIR "/src/RabbitMQDemo.Background"
RUN dotnet build "./RabbitMQDemo.Background.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./RabbitMQDemo.Background.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "RabbitMQDemo.Background.dll"]
