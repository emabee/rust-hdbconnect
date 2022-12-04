# For a HANA in SAP Cloud Platform, get PEM Certificate for TLS connections

## Acquire HANA Pem Certificate manually, e.g. to access HANA from outside CloudFoundry

### Requirements

- You have a global account on the SAP Cloud Platform Cockpit (not a trial account)
  that runs on CloudFoundry
- Your database was provisioned after June 4, 2018
  ([here](https://help.sap.com/viewer/cc53ad464a57404b8d453bbadbc81ceb/Cloud/en-US/7cc0278fa13c4124bfe6af2ae5b59642.html)’s
  a guide how to find out, section
  ‘How Can I Find Out if My Databases Were Provisioned Before or After June 4, 2018?’)
- You have a space in which you have set up a `hana-db` service instance

### Steps to get the certificate

- Navigate to your space on your global account
- In the sidebar click on ‘Service Instances’
- Click on the name of your `hana-db` service instance
- In the sidebar click on ‘Service Keys’
- Click on ‘Create Service Key’ if necessary
- The page will display some JSON; copy the value under `certificate`
- Paste into a text editor
- Make sure to replace all occurences of `\n` with a new line
- Save as a pem file
