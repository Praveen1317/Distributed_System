const express = require('express');
const multer = require('multer');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const crypto = require('crypto');

const app = express();
const PORT = 8080;

// Enable CORS
app.use(cors());

// Set up storage nodes
const NODES = ['storage_node_1', 'storage_node_2', 'storage_node_3'];
NODES.forEach(node => {
    if (!fs.existsSync(node)) {
        fs.mkdirSync(node);
    }
});

// Set up SQLite database
const db = new sqlite3.Database('database.sqlite', (err) => {
    if (err) {
        console.error(err.message);
    }
    db.run(`CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT NOT NULL,
        hash TEXT NOT NULL,
        replicas INTEGER NOT NULL
    )`);
});

app.get('/', (req, res) => {
    res.send('Welcome to the Distributed File Storage System!');
});

// File upload configuration
const storage = multer.memoryStorage();
const upload = multer({ storage });

// Helper function to calculate file hash
function calculateHash(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
}

// Upload endpoint
app.post('/upload', upload.single('file'), (req, res) => {
    if (!req.file) {
        return res.status(400).send('No file uploaded.');
    }

    const { originalname, buffer } = req.file;
    const fileHash = calculateHash(buffer);

    // Save file in each storage node
    NODES.forEach(node => {
        const filePath = path.join(node, originalname);
        fs.writeFileSync(filePath, buffer); // Save the file
    });

    // Save metadata in the database
    db.run(`INSERT INTO files (filename, hash, replicas) VALUES (?, ?, ?)`, 
        [originalname, fileHash, NODES.length], function(err) {
        if (err) {
            console.error(err);
            return res.status(500).json({ error: 'Database error' });
        }
        res.status(201).json({ message: 'File uploaded successfully', filename: originalname, hash: fileHash });
    });
});

// List files endpoint
app.get('/files', (req, res) => {
    db.all(`SELECT filename, hash, replicas FROM files`, [], (err, rows) => {
        if (err) {
            return res.status(500).json({ error: 'Database error' });
        }
        res.json(rows);
    });
});

// Download endpoint
app.get('/download/:filename', (req, res) => {
    const filename = req.params.filename;
    let found = false;

    NODES.forEach(node => {
        const filePath = path.join(node, filename);
        if (fs.existsSync(filePath)) {
            found = true;
            return res.download(filePath, filename); // Send file for download
        }
    });

    if (!found) {
        return res.status(404).json({ error: 'File not found' });
    }
});

// Start the server
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
