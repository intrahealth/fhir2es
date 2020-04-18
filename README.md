# fhir2es
# Installation
```
npm i fhir2es
```

# Example Usage
```
const { CacheFhirToES } = require('fhir2es')
const env = process.env.NODE_ENV || 'development';
var config = require(`emNutt/server/config/config_${env}.json`);

let caching = new CacheFhirToES({
  ESBaseURL: config.elastic.baseURL,
  ESUsername: config.elastic.username,
  ESPassword: config.elastic.password,
  ESMaxCompilationRate: config.elastic.max_compilations_rate,
  FHIRBaseURL: config.macm.baseURL,
  FHIRUsername: config.macm.username,
  FHIRPassword: config.macm.password
})
caching.cache()
```
