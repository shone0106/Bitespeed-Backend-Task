const express = require('express');
const mysql = require('mysql2/promise');
const dotenv = require('dotenv')

dotenv.config()
const app = express();
app.use(express.json());

const dbConfig = {
    host: process.env.HOST,
    user: process.env.USER,
    password: process.env.PASSWORD,
    database: process.env.DATABASE
};

const pool = mysql.createPool(dbConfig);

async function getContacts(email, phoneNumber) {
    let query = 'SELECT * FROM Contact WHERE ';
    const params = [];
    if (email) {
        query += 'email = ? ';
        params.push(email);
    }
    if (phoneNumber) {
        if (email) query += 'OR ';
        query += 'phoneNumber = ?';
        params.push(phoneNumber);
    }
    const [rows] = await pool.query(query, params);
    return rows;
}

async function insertContact(email, phoneNumber, linkedId, linkPrecedence) {
    const [result] = await pool.query(
        'INSERT INTO Contact (email, phoneNumber, linkedId, linkPrecedence) VALUES (?, ?, ?, ?)',
        [email, phoneNumber, linkedId, linkPrecedence]
    );
    return result.insertId;
}

async function updateContact(id, linkedId, linkPrecedence) {
    await pool.query(
        'UPDATE Contact SET linkedId = ?, linkPrecedence = ? WHERE id = ?',
        [linkedId, linkPrecedence, id]
    );
}


async function findByField(field, value) {
    const validFields = ['email', 'phoneNumber', 'id', 'linkedId'];
    if (!validFields.includes(field)) {
        throw new Error('Invalid field name');
    }

    const query = `SELECT * FROM Contact WHERE ${field} = ?`;
    const [result] = await pool.query(query, [value]);
    return result;
    
}


app.post('/identify', async (req, res) => {
    const { email, phoneNumber } = req.body;
   
    try {
        let contacts = await getContacts(email, phoneNumber);
        console.log('Matching entries with given email or phonenumber')
        console.log(contacts)
        if (contacts.length === 0) {
            const newContactId = await insertContact(email, phoneNumber, null, 'primary');
            res.json({
                contact: {
                    primaryContactId: newContactId,
                    emails: [email],
                    phoneNumbers: [phoneNumber],
                    secondaryContactIds: []
                }
            });
        } else {
            let primaryContacts = contacts.filter(c => c.linkPrecedence === 'primary');
            let primaryContact = null

            if (primaryContacts.length == 0) {
                let primaryId = contacts[0].linkedId
                console.log('logging primaryId')
                console.log(primaryId)
                primaryContacts = await findByField('id',primaryId)
            }

           
                primaryContact = primaryContacts[0]
                if (primaryContacts.length > 1) {
                    primaryContact = primaryContacts.reduce((prev, current) => 
                        (prev.createdAt < current.createdAt) ? prev : current)
                }
                    
            

           
            const primaryContactId = primaryContact.id;
            const secondaryContactIds = new Set();
            const emails = new Set([primaryContact.email]);
            const phoneNumbers = new Set([primaryContact.phoneNumber]);
            
            console.log(`primary contact id : ${primaryContactId}`)
            let secondaryContacts = await findByField('linkedId', primaryContactId)
            console.log(secondaryContacts)

            contacts = [...new Set([...secondaryContacts, ...contacts ])]
            contacts.push(primaryContact)

            console.log('logging all contacts of the user')
            console.log(contacts)


            for (const contact of contacts) {
                if (contact.id !== primaryContactId) {
                    if (contact.linkPrecedence === 'primary') {
                        await updateContact(contact.id, primaryContactId, 'secondary');
                    }
                    secondaryContactIds.add(contact.id);
                    emails.add(contact.email);
                    phoneNumbers.add(contact.phoneNumber);
                }
            }

            const emailExists = contacts.some(c => c.email === email);
            const phoneNumberExists = contacts.some(c => c.phoneNumber === phoneNumber);

            if ((!emailExists && email) || (!phoneNumberExists && phoneNumber)) {
                const newContactId = await insertContact(email, phoneNumber, primaryContactId, 'secondary');
                secondaryContactIds.add(newContactId);
                emails.add(email);
                phoneNumbers.add(phoneNumber);
            }

            res.json({
                contact: {
                    primaryContactId,
                    emails: Array.from(emails).filter(Boolean),
                    phoneNumbers: Array.from(phoneNumbers).filter(Boolean),
                    secondaryContactIds: Array.from(secondaryContactIds).filter(Boolean)
                }
            });
        }
    } catch (error) {
        console.error('Error identifying contact:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

app.listen(process.env.PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
