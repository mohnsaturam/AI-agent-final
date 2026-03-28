import React from 'react'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import HomePage from './pages/HomePage'
import RequestDetailPage from './pages/RequestDetailPage'
import Layout from './components/Layout'

function App() {
    return (
        <Router>
            <Layout>
                <Routes>
                    <Route path="/" element={<HomePage />} />
                    <Route path="/requests/:requestId" element={<RequestDetailPage />} />
                </Routes>
            </Layout>
        </Router>
    )
}

export default App
