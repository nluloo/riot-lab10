FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# .csproj отдельно от кода: restore не перезапускается на каждой правке .cs
COPY Shared/Shared.csproj Shared/
COPY Nodes/Nodes.csproj Nodes/
RUN dotnet restore Nodes/Nodes.csproj

COPY Shared/ Shared/
COPY Nodes/ Nodes/
RUN dotnet publish Nodes/Nodes.csproj -c Release -o /publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /publish ./

EXPOSE 6001
ENTRYPOINT ["dotnet", "Nodes.dll"]
