FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY DistributedSystem/Shared/Shared.csproj DistributedSystem/Shared/
COPY DistributedSystem/Server/Server.csproj DistributedSystem/Server/
RUN dotnet restore DistributedSystem/Server/Server.csproj

COPY DistributedSystem/Shared/ DistributedSystem/Shared/
COPY DistributedSystem/Server/ DistributedSystem/Server/
RUN dotnet publish DistributedSystem/Server/Server.csproj -c Release -o /publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /publish ./

EXPOSE 5000
ENTRYPOINT ["dotnet", "Server.dll"]
