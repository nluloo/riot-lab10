FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# .csproj отдельно от кода: restore не перезапускается на каждой правке .cs
COPY Shared/Shared.csproj Shared/
COPY Client/Client.csproj Client/
RUN dotnet restore Client/Client.csproj

COPY Shared/ Shared/
COPY Client/ Client/
RUN dotnet publish Client/Client.csproj -c Release -o /publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /publish ./

EXPOSE 5000
ENTRYPOINT ["dotnet", "Client.dll"]
