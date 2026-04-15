import 'dotenv/config'
import Airtable from 'airtable'

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID)

const records = await base(process.env.AIRTABLE_TABLE_ID).select({
  returnFieldsByFieldId: true,
  filterByFormula: `{fldgmm7QkRDt2e9nC} = 'Saucony_S20756_16_Size_8M_195018774231_UGD'`,
  fields: ['fldHDhUU6MNrQUORt', 'fldxI3jS2HxLbtFXT', 'fldur2N3IOR2rdlf8', 'fldjJAFcnpntaN7l4'],
  maxRecords: 1
}).firstPage()

for (const r of records) {
  const f = r.fields
  console.log('Price:', f['fldHDhUU6MNrQUORt'])
  console.log('item_cost:', f['fldxI3jS2HxLbtFXT'])
  console.log('Variant Price:', f['fldur2N3IOR2rdlf8'])
  console.log('AI Cost Check:', f['fldjJAFcnpntaN7l4']?.name)
}
