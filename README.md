# fhir2es
# Installation
```
npm i fhir2es
```

# Example Usage
```
const { CacheFhirToES } = require('fhir2es')

let caching = new CacheFhirToES({
  ESBaseURL: 'http://localhost:9200',
  ESUsername: '',
  ESPassword: '',
  ESMaxCompilationRate: '10000/1m',
  FHIRBaseURL: 'http://localhost:8081/hapi/fhir',
  FHIRUsername: '',
  FHIRPassword: ''
})
caching.cache()
```
