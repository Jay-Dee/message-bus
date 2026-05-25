FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy project files from root context for dependency restoration
COPY ["src/MessageBus-Consumer/MessageBus-Consumer.csproj", "MessageBus-Consumer/"]
RUN dotnet restore "MessageBus-Consumer/MessageBus-Consumer.csproj"

# Copy remaining source code and compile
COPY src/ .
WORKDIR "/src/MessageBus-Consumer"
RUN dotnet publish "MessageBus-Consumer.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Final lightweight runtime stage
FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "MessageBus-Consumer.dll"]
