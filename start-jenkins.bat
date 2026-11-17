@echo off
set "SCRIPT_DIR=%~dp0"
set "JENKINS_HOME=%SCRIPT_DIR%jenkins\jenkins_home"
echo Starting Jenkins on http://localhost:8085 ...
java "-Djenkins.install.runSetupWizard=false" "-Dhudson.plugins.git.GitSCM.ALLOW_LOCAL_CHECKOUT=true" -jar "%SCRIPT_DIR%jenkins\jenkins.war" --httpPort=8085
