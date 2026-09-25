using 'main.bicep'

param prefix             = 'aca-sandbox'
param location           = readEnvironmentVariable('AZURE_LOCATION', 'westus3')
param openAiLocation     = readEnvironmentVariable('AZURE_OPENAI_LOCATION', 'westus3')
param gpt5MiniModelVersion = ''
param gpt5MiniCapacity     = 50
param acrSku             = 'Basic'

// On the FIRST deployment, leave these empty — the Bicep provisions the
// orchestrator app with a placeholder image. After the infra exists, run
// setup.sh to push real images and update the app in-place.
//
// You can also pre-build images and pass them here for a one-shot deploy:
// param orchestratorImage  = '<acr-name>.azurecr.io/agent-orchestrator:latest'
// param researchAgentImage = '<acr-name>.azurecr.io/research-agent:latest'
