import uuid
import hashlib
import base64

def generate_code_verifier_challenge():
    code_verifier = str(uuid.uuid4()).upper() + "-" + str(uuid.uuid4()).upper()
    code_challenge = base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest()).decode('utf-8')
    code_challenge = code_challenge.replace('=', '')
    return code_verifier, code_challenge