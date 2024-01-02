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
  ESMaxScrollContext: '100000',
  FHIRBaseURL: 'http://localhost:8081/hapi/fhir',
  FHIRUsername: '',
  FHIRPassword: '',
  relationshipsIDs: [] //if not specified then all relationships will be processed
  reset: false //default to false
})
caching.cache()
```

# Creating Relationships

Create a Basic resource with a profile id iHRISRelationship

```
{
  resourceType: "Basic",
  id: "ihris-es-report-staffs",
  meta: {
    profile: ["http://ihris.org/StructureDefinition/iHRISRelationship"]
  }
}
```

The relationship has two main parts. The Primary resource and linked resources. Linked resources can link to each other i.e if the primary resource is Practitioner, you can then link PractitionerRole to practitioner using PractitionerRole.practitioner and later on link the Location resource to PractitionerRole using PractitionerRole.location. Different resources are linked together using references.

A resources can be directly linked or reversed linked i.e if Practitioner resource is added before PractitionerRole in the relationship, then PractitionerRole can be added as a reverse link since Practitioner has no reference to PractitionerRole but PractitionerRole has a reference to Practitioner. But then, on this same example, the Location resource can be added as a direct link since PractitionerRole has a reference to a Location

To link a resource as a reverse link, you must provide a search parameter, where as for direct link you dont need to provide a search parameter. Providing a search parameter is shown below.

## Adding a primary resource

To add a primary resource, add a complex extension with url <http://ihris.org/StructureDefinition/iHRISReportDetails>

```
{
  resourceType: "Basic",
  id: "ihris-es-report-staffs",
  meta: {
    profile: ["http://ihris.org/StructureDefinition/iHRISRelationship"]
  },
  extension: [{
    url: "http://ihris.org/StructureDefinition/iHRISReportDetails",
    extension: []
  }]
}
```

Since iHRISReportDetails is a complex extension, then it supports sub extensions and below are the possible subextensions that can be defined for the iHRISReportDetails

- resource - This is the resource name to be used as the primary resource i.e Practitioner. This is mandatory.
- name - This is the unique name of the primary resource, which will also be the name of the elasticsearch index. Other linked resources will use this name to reference this primary resource. This is mandatory
- label - This is the display name of the entire report. This is mandatory.
- query - This is the fhirpath to be used to filter out unwanted resources. i.e you may only be interested to have a report of female Practitioner, then for that case you will define a fhirpath like Practitioner.gender=female which will only allow female practitioners to be on the report. This is optional.
- initialFilter - This works the same way as query, but this is the search parameter and it is always run only once when the report is cached for the first time. This was intended to improve caching speed. Take an example you want to cache Basic resources of profile type professions, without initialFilter, fhir2es will pull all the Basic resources (could be a thousand of them) and start processing each of them by applying a <b>query</b> to them.
But if initialFilter is defined i.e _profile=professions, fhir2es will only pull Basic resources of profile type professions and start processing them. Since initialFilter is defined only once, then query must always be difined when initialFilter is defined and it must be a fhirpath relative to the initialFilter.
The reason initialFilter is run only once, is because resources can be modified and we would want to remove from report/elastcisearch all the resources that no longer meets the condition.
initialFilter is optional
- cachingDisabled - Set this to true if you dont want fhir2es to cache data for this relationship and you can cache data on your own approach
- displayCheckbox - Set this to true if you want the report to have checkboxes for users to select a row
- locationBasedConstraint - Set this to true if you want to restrict data access by user location
- IhrisReportElement - This is a complex extension which now defines all the fields of this resource that you want to be available on your report. This will be covered on a separate section of its own.

Here is an example for what we have covered so far.

```
{
  resourceType: "Basic",
  id: "ihris-es-report-staffs",
  meta: {
    profile: ["http://ihris.org/StructureDefinition/iHRISRelationship"]
  },
  extension: [{
    url: "http://ihris.org/StructureDefinition/iHRISReportDetails",
    extension: [{
      url: "label",
      valueString: "Practitioners License Registration"
    }, {
      url: "displayCheckbox",
      valueBoolean: false
    }, {
      url: "name",
      valueString: "licenseregistration"
    }, {
      url: "locationBasedConstraint",
      valueBoolean: true
    }, {
      url: "resource",
      valueString: "Basic"
    }, {
      "url": "initialFilter",
      "valueString": "_profile=http://ihris.org/fhir/StructureDefinition/registration-license-profile"
    }, {
      url: "query",
      valueString: "Basic.meta.profile.contains('http://ihris.org/fhir/StructureDefinition/registration-license-profile')"
    }]
  }]
}
```

## Linking another resource into a relationship

You are allowed to link together as many resources as possible depending with your usecase. You just need to know what connects the resource you are trying to add with a resource already existing into a relationship. From our above example, the Basic resource we have added have a link to the Practitioner resource via extension.where(url="<http://ihris.org/fhir/StructureDefinition/ihris-practitioner-reference").valueReference.reference>. This allows us to add the Practitioner resource into the relationship. To link a new resource into the relationship we use iHRISReportLink complex extension

Since iHRISReportLink is a complex extension, then it supports sub extensions and below are the possible subextensions that can be defined for the iHRISReportLink

- resource - This is the resource name to be linked i.e Practitioner. This is mandatory.
- name - This is the unique name of the resource being linked into the relationship, doesnt need to be the same as the resource name. Other linked resources will use this name to reference this linked resource. This is mandatory
- query - This is the fhirpath to be used to filter out unwanted resources. It works the same way to what we saw in the primary resource above. This is optional.
- initialFilter - This works the same way to what we saw in the primary resource above. This is optional
- linkElement - The element of the resource we are linking that connects with the resource in the relationship. For our example this will be Practitioner.id
- linkTo - The resource element within the relationship to which the resource we are linking connects to, we refer to this resource using the name we gave it in the relationship, this is because more than one resource of the same type can be added into the relationship. For our example linkTo will be licenseregistration.extension.where(url='<http://ihris.org/fhir/StructureDefinition/ihris-practitioner-reference').valueReference.reference>
- linkElementSearchParameter - This is the search parameter for the resource in the relationship in relation to the resource you are linking. And this must be specified only when the resource you are linking has reverse relation to the resource in the relationship i.e if Practitioner is in the relationship and you waant to link PractitionerRole, then this is the reverse link and search parameter must be specified via linkElementSearchParameter
- IhrisReportElement - This is a complex extension which now defines all the fields of this resource that you want to be available on your report. This will be covered on a separate section of its own.

Here is an example of how a link can be defined

```
{
  "url": "http://ihris.org/fhir/StructureDefinition/iHRISReportLink",
  "extension": [{
    "url": "name",
    "valueString": "practitioner"
  }, {
    "url": "resource",
    "valueString": "Practitioner"
  }, {
    "url": "query",
    "valueString": "Practitioner.meta.tag.where(system='http://ihris.org/fhir/CodeSystem/boards' and code='nmcb').exists()"
  }, {
    "url": "initialFilter",
    "valueString": "_tag=nmcb"
  }, {
    "url": "linkElement",
    "valueString": "Practitioner.id"
  }, {
    "url": "linkTo",
    "valueString": "licenseregistration.extension.where(url='http://ihris.org/fhir/StructureDefinition/ihris-practitioner-reference').valueReference.reference"
  }, {
    "url": "multiple",
    "valueBoolean": false
  }]
}
```

## Adding resource/report fields

Fields of a resource can be added using iHRISReportElement complex extension which is available for both the primary resource and the linked resource. Since iHRISReportElement is a complext extension, it has below subextensions to help you define your field.

- display - This is the display name of the field that will appear on the report.
- name - This is the field unique name on the entire relationship.
- fhirpath - This is the fhirpath expression for field value on the relationship
- displayFormat - This can be defined if you are intending to modify or format the way field value is displayed i.e Adding extra texts to a value or even combining two fields values into a single display. This is a complex extension with below subextensions
  - format - This defines how you want to format the display, parameters are defined with %s. i.e if you are displaying age and you want the value to appear as 16 Years Old, the format will be %s Years Old, %s will be replaced with a respecitve age. Or if you want to display Full name instead of firstname and othernames separately, you can do %s %s as seen in below example
  - order - This defines the parameter names in an order as defined by %s in the format element above, i.e if you had %s %s in the format, then ths means you must have two parameter names in the order separated by coma i.e given,family. This is telling fhir2es to replace the first %s in the format element with given and replace the second %s with family
  - paths:parameter_name:fhirpath - This now defines a fhirpath expression of all the parameter names defined with the order element. i.e in above we have given,family in order element, this means we must define fhirpath for both i.e paths:given:fhirpath = name.where(use='official').given and paths:family:fhirpath = name.where(use='official').family.
  - paths:parameter_name:join - This is optional, and it is used to join an array of values when a field is expected to contain an array of values. i.e we know given is an array, you can define paths:given:join = ", " to join given names with coma.
  displayFormat and fhirpath cant be defined together, you either define fhirpath or you define displayFormat
- function - fhir2es allows you to define your own custom functions and use them to calculate field value. functions are defined within a module and they do accepts parameters. If you are running fhir2es inside iHRIS then your module function must be defined inside your custom site under the path iHRIS/ihris-backend/sitename/modules/es. If you are running fhir2es outside iHRIS then you will have to define manually the base path to your modules, and this is done when creating fhir2es class using ESModulesBasePath: "/home/ally/mysoftware/modules/es". i.e

```
const { CacheFhirToES } = require('./reports')
let caching = new CacheFhirToES({
  ESBaseURL: 'http://localhost:9200',
  ESUsername: '',
  ESPassword: '',
  ESMaxCompilationRate: '100000/1m',
  ESMaxScrollContext: '100000',
  FHIRBaseURL: 'http://localhost:8081/bwihris/fhir',
  FHIRUsername: '',
  FHIRPassword: '',
  since: '', //use this to specify last updated time of resources to be processed
  relationshipsIDs: ['ihris-es-report-pp-staffs'], //if not specified then all relationships will be processed
  reset: true, //will pull all resources if set to true
  ESModulesBasePath: "/home/ally/mysoftware/modules/es"
})
caching.cache().then(() => {
  console.log('Done')
})
```

So if you want to calculate age from date of birth, you will first need to create a nodejs module file in the format as defined below

```

const testmodule = {
  
}

module.exports = testmodule
```

Then from there you can define your functions within the module as in below

```
const moment = require("moment")

const testmodule = {
  age: (fields) => {
    return new Promise((resolve, reject) => {
      let age = moment().diff(fields.dob, 'years');
      resolve(age)
    })
  }
}

module.exports = testmodule
```

You are allowed to have multiple functions within the module.
**Module functions must return a promise.**
Within the relationship file, a module function is defined by specifying the name of the file containing you functions, followed by the function name and any required parameters i.e testmodule.age({dob}).
Parameter names are the fields that are already added on your relationship, it could be within the same linked resource or other resources within the relationship.
