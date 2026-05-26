FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

EXPOSE 8080

# Copy project files from root context for dependency restoration
COPY ["src/MessageBus.Producer/MessageBus.Producer.csproj", "MessageBus.Producer/"]
RUN dotnet restore "MessageBus.Producer/MessageBus.Producer.csproj"

# Copy remaining source code and compile
COPY src/ .
WORKDIR "/src/MessageBus.Producer"
RUN dotnet publish "MessageBus.Producer.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Final lightweight runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app

EXPOSE 8080
ENV ASPNETCORE_URLS=http://+:8080

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "MessageBus.Producer.dll"]
