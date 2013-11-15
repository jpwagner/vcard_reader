import re
from db import models

# vcf_file = 'data/4others.vcf'
# vcf_file = 'data/00001.vcf'
# vcf_file = 'data/00002.vcf'
vcf_file = 'data/00003.vcf'


with open(vcf_file, 'r') as f:
	data = f.read()
	f.close()

entries = data.split('BEGIN:VCARD')[1:]

contacts = []
for entry in entries:
	vcard_json = dict(tuple(re.split(':', e, maxsplit=1)) for e in entry.strip().split('\r\n') if ':' in e and 'PHOTO' not in e)
	vcard_json.pop('END', None)	
	name = vcard_json.get('FN')

	session = models.create_db_session()

	c = models.Contact(name=name)
	matches = c.duplicates(session)
	if not matches:
		session.add(c)
		# print c.name
	else:
		c = matches[0]
	session.commit()

	v = models.Vcard(vcard=vcard_json, contact_id=c.id)
	session.add(v)

	for key in vcard_json.keys():
		if 'EMAIL' in key:
			e = models.Email(address=vcard_json[key], contact_id=c.id)
			matches = e.duplicates(session)
			if not matches:
				session.add(e)
			# attributes --> key.split(';')[1:])
		if 'TEL' in key:
			p = models.Phone(number=vcard_json[key], contact_id=c.id)
			matches = p.duplicates(session)
			if not matches:
				session.add(p)
		if 'ADR' in key:
			a = models.Address(address=vcard_json[key], contact_id=c.id)
			matches = a.duplicates(session)
			if not matches:
				session.add(a)
		if 'URL' in key:
			w = models.Webpage(address=vcard_json[key], contact_id=c.id)
			matches = w.duplicates(session)
			if not matches:
				session.add(w)

	session.commit()
	models.end_db_session(session)




