import os
from azure.identity import AzurePowerShellCredential
from apicostmanagement import get_enroll_list
from dotenv import load_dotenv


load_dotenv()

credential = AzurePowerShellCredential(tenant_id=os.getenv("AZURE_TENANT_ID"))

# TODO: this method generate a token that not authorized for any API calls
credential_token = credential.get_token("https://management.azure.com/")
print(get_enroll_list(credential_token.token))
