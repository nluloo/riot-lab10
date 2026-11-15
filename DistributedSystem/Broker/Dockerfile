FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# .csproj отдельно от кода: restore не перезапускается на каждой правке .cs
COPY Shared/Shared.csproj Shared/
COPY Broker/Broker.csproj Broker/
RUN dotnet restore Broker/Broker.csproj

COPY Shared/ Shared/
COPY Broker/ Broker/
RUN dotnet publish Broker/Broker.csproj -c Release -o /publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /publish ./

EXPOSE 5001
ENTRYPOINT ["dotnet", "Broker.dll"]
