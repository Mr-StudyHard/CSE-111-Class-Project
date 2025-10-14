import { useEffect, useState } from 'react'
import { api } from './api'

export default function App() {
  const [msg, setMsg] = useState('Loading…')

  useEffect(() => {
    api.get('/health')
      .then(r => setMsg(JSON.stringify(r.data)))
      .catch(e => setMsg(`Error: ${e}`))
  }, [])

  return (
    <div style={{ maxWidth: 720, margin: '40px auto', padding: 16 }}>
      <h1>Vite ↔ Flask wiring check</h1>
      <p>API says: {msg}</p>
      <p style={{opacity:.7}}>Try hitting /api/ping too (it checks DB connectivity).</p>
    </div>
  )
}
