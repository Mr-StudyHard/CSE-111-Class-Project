import { useEffect, useMemo, useState } from 'react'
import './App.css'
import { getSummary, getList, refresh, search, type MediaItem, getUsers, type UserRow, getHealth, login, signup } from './api'

type Tab = 'home' | 'analytics' | 'movies' | 'tv' | 'search'
type View = 'app' | 'auth' | 'login' | 'signup' | 'accounts'
type TrendingPeriod = 'weekly' | 'monthly' | 'all'

const TRENDING_MOCK: Record<TrendingPeriod, MediaItem[]> = {
  weekly: [
    {
      tmdb_id: 101,
      media_type: 'movie',
      title: 'The Last Horizon',
      overview: 'An elite crew ventures beyond the solar frontier to rescue a stranded science vessel before it disappears into a cosmic anomaly.',
      vote_average: 8.4,
      release_date: '2024-08-15',
      genres: ['Sci-Fi', 'Adventure'],
      poster_path: 'https://images.unsplash.com/photo-1582719478250-c89cae4dc85b?auto=format&fit=crop&w=300&q=80',
      backdrop_path: 'https://images.unsplash.com/photo-1523419409543-0c1df022bdd1?auto=format&fit=crop&w=1280&q=80',
      original_language: 'en',
    },
    {
      tmdb_id: 102,
      media_type: 'tv',
      title: 'Neon Nights',
      overview: 'A journalist uncovers a conspiracy inside a mega-corporation while navigating an electric, neon-lit metropolis.',
      vote_average: 7.9,
      genres: ['Thriller', 'Drama'],
      poster_path: 'https://images.unsplash.com/photo-1500530855697-b586d89ba3ee?auto=format&fit=crop&w=300&q=80',
      backdrop_path: 'https://images.unsplash.com/photo-1529429617124-aee711a21d46?auto=format&fit=crop&w=1280&q=80',
      release_date: '2024-05-10',
      original_language: 'en',
    },
    {
      tmdb_id: 103,
      media_type: 'movie',
      title: 'Legends of Aether',
      overview: 'Heroes from distant realms unite to protect a floating archipelago powered by ancient magic.',
      vote_average: 8.1,
      genres: ['Fantasy'],
      poster_path: 'https://images.unsplash.com/photo-1518709268805-4e9042af9f23?auto=format&fit=crop&w=300&q=80',
      backdrop_path: 'https://images.unsplash.com/photo-1528825871115-3581a5387919?auto=format&fit=crop&w=1280&q=80',
      release_date: '2024-04-02',
      original_language: 'en',
    },
    {
      tmdb_id: 104,
      media_type: 'tv',
      title: 'Atlas Station',
      overview: 'The crew of a deep-space research hub faces an unknown organism with its own intelligence.',
      vote_average: 8.7,
      genres: ['Sci-Fi', 'Mystery'],
      poster_path: 'https://images.unsplash.com/photo-1492724441997-5dc865305da7?auto=format&fit=crop&w=300&q=80',
      backdrop_path: 'https://images.unsplash.com/photo-1475694867812-f82b8696d610?auto=format&fit=crop&w=1280&q=80',
      release_date: '2024-03-17',
      original_language: 'en',
    },
  ],
  monthly: [
    {
      tmdb_id: 201,
      media_type: 'movie',
      title: 'Golden Summer',
      overview: 'A heartfelt drama following three friends chasing their dreams along Italy’s sunlit coast.',
      vote_average: 7.6,
      genres: ['Drama', 'Romance'],
      poster_path: 'https://images.unsplash.com/photo-1500530855697-b586d89ba3ee?auto=format&fit=crop&w=300&q=80',
      backdrop_path: 'https://images.unsplash.com/photo-1476480862126-209bfaa8edc8?auto=format&fit=crop&w=1280&q=80',
      release_date: '2023-07-21',
      original_language: 'it',
    },
    {
      tmdb_id: 202,
      media_type: 'tv',
      title: 'Echoes of Terra',
      overview: 'An ecological mystery series charting humanity’s desperate attempt to heal a fractured planet.',
      vote_average: 8.0,
      genres: ['Drama', 'Sci-Fi'],
      poster_path: 'https://images.unsplash.com/photo-1478720568477-152d9b164e26?auto=format&fit=crop&w=300&q=80',
      backdrop_path: 'https://images.unsplash.com/photo-1469474968028-56623f02e42e?auto=format&fit=crop&w=1280&q=80',
      release_date: '2023-11-03',
      original_language: 'en',
    },
    {
      tmdb_id: 203,
      media_type: 'movie',
      title: 'Midnight Pulse',
      overview: 'A pulse-pounding heist thriller unfolding in real time across downtown Seoul.',
      vote_average: 8.2,
      genres: ['Action', 'Thriller'],
      poster_path: 'https://images.unsplash.com/photo-1500534307688-6023f12e02b2?auto=format&fit=crop&w=300&q=80',
      backdrop_path: 'https://images.unsplash.com/photo-1526481280695-3c4697e2f82e?auto=format&fit=crop&w=1280&q=80',
      release_date: '2024-01-12',
      original_language: 'ko',
    },
    {
      tmdb_id: 204,
      media_type: 'tv',
      title: 'Starlight Academy',
      overview: 'Young virtuosos compete at an elite performing arts school orbiting Earth.',
      vote_average: 7.8,
      genres: ['Drama', 'Music'],
      poster_path: 'https://images.unsplash.com/photo-1521737604893-d14cc237f11d?auto=format&fit=crop&w=300&q=80',
      backdrop_path: 'https://images.unsplash.com/photo-1500530855697-b586d89ba3ee?auto=format&fit=crop&w=1280&q=80',
      release_date: '2023-09-06',
      original_language: 'en',
    },
  ],
  all: [
    {
      tmdb_id: 301,
      media_type: 'movie',
      title: 'Riftwalkers',
      overview: 'Veteran explorers guard ancient portals that connect distant civilizations in space and time.',
      vote_average: 8.9,
      genres: ['Sci-Fi', 'Adventure'],
      poster_path: 'https://images.unsplash.com/photo-1519125323398-675f0ddb6308?auto=format&fit=crop&w=300&q=80',
      backdrop_path: 'https://images.unsplash.com/photo-1498050108023-c5249f4df085?auto=format&fit=crop&w=1280&q=80',
      release_date: '2022-05-19',
      original_language: 'en',
    },
    {
      tmdb_id: 302,
      media_type: 'tv',
      title: 'Oracle City',
      overview: 'In a metropolis where future crimes are predicted, a detective questions the system’s true motives.',
      vote_average: 9.1,
      genres: ['Sci-Fi', 'Drama'],
      poster_path: 'https://images.unsplash.com/photo-1492724441997-5dc865305da7?auto=format&fit=crop&w=300&q=80',
      backdrop_path: 'https://images.unsplash.com/photo-1498050108023-c5249f4df085?auto=format&fit=crop&w=1280&q=80',
      release_date: '2021-10-08',
      original_language: 'en',
    },
    {
      tmdb_id: 303,
      media_type: 'movie',
      title: 'Harbor Lights',
      overview: 'A sweeping romance that follows two strangers brought together by a century-old mystery.',
      vote_average: 8.5,
      genres: ['Romance', 'Mystery'],
      poster_path: 'https://images.unsplash.com/photo-1489515217757-5fd1be406fef?auto=format&fit=crop&w=300&q=80',
      backdrop_path: 'https://images.unsplash.com/photo-1446776811953-b23d57bd21aa?auto=format&fit=crop&w=1280&q=80',
      release_date: '2022-02-14',
      original_language: 'en',
    },
    {
      tmdb_id: 304,
      media_type: 'tv',
      title: 'Infinite Echo',
      overview: 'An anthology series chronicling humanity’s encounters with mysterious signals from the cosmos.',
      vote_average: 8.7,
      genres: ['Sci-Fi', 'Anthology'],
      poster_path: 'https://images.unsplash.com/photo-1446776811953-b23d57bd21aa?auto=format&fit=crop&w=300&q=80',
      backdrop_path: 'https://images.unsplash.com/photo-1482192597420-4817fdd7e8b0?auto=format&fit=crop&w=1280&q=80',
      release_date: '2020-04-28',
      original_language: 'en',
    },
    {
      tmdb_id: 305,
      media_type: 'movie',
      title: 'Edge of Silence',
      overview: 'A lone spy must decide who to trust when a mission to expose corruption turns personal.',
      vote_average: 8.3,
      genres: ['Thriller'],
      poster_path: 'https://images.unsplash.com/photo-1517602302552-471fe67acf66?auto=format&fit=crop&w=300&q=80',
      backdrop_path: 'https://images.unsplash.com/photo-1504384308090-c894fdcc538d?auto=format&fit=crop&w=1280&q=80',
      release_date: '2021-12-09',
      original_language: 'en',
    },
  ],
}

function Stat({label, value}:{label:string;value:React.ReactNode}){
  return (
    <div className="stat">
      <div className="stat-value">{value}</div>
      <div className="stat-label">{label}</div>
    </div>
  )
}

function Card({item}:{item:MediaItem}){
  const img = item.poster_path ? `https://image.tmdb.org/t/p/w300${item.poster_path}` : undefined
  return (
    <div className="card">
      {img ? <img src={img} alt={item.title} loading="lazy"/> : <div className="noimg">No image</div>}
      <div className="card-body">
        <div className="chip">{item.media_type.toUpperCase()}</div>
        <h4 title={item.title}>{item.title}</h4>
        <div className="meta">
          <span>⭐ {item.vote_average?.toFixed?.(1) ?? '–'}</span>
          <span>🌐 {item.original_language?.toUpperCase?.() ?? '—'}</span>
          <span>📅 {item.release_date ?? '—'}</span>
        </div>
      </div>
    </div>
  )
}

export default function App() {
  const [tab, setTab] = useState<Tab>('home')
  const [view, setView] = useState<View>('app')
  const [busy, setBusy] = useState(false)
  const [summary, setSummary] = useState<any>()
  const [movies, setMovies] = useState<MediaItem[]>([])
  const [tv, setTv] = useState<MediaItem[]>([])
  const [q, setQ] = useState('')
  const [results, setResults] = useState<MediaItem[]>([])
  const [username, setUsername] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [accounts, setAccounts] = useState<UserRow[]>([])
  const [accountsError, setAccountsError] = useState<string | null>(null)
  const [backendOnline, setBackendOnline] = useState<boolean | null>(null)
  const [trendingPeriod, setTrendingPeriod] = useState<TrendingPeriod>('weekly')
  const [carouselIndex, setCarouselIndex] = useState(0)
  // Remember-me support: if a profile was stored and flag set, restore it.
  const [currentUser, setCurrentUser] = useState<{user:string;email:string}|null>(() => {
    try {
      const flag = localStorage.getItem('rememberUser') === '1'
      if(!flag) return null
      const raw = localStorage.getItem('currentUser')
      return raw? JSON.parse(raw) : null
    } catch { return null }
  })
  const [remember, setRemember] = useState<boolean>(() => localStorage.getItem('rememberUser') === '1')
  const [loginError, setLoginError] = useState<string | null>(null)
  const [signupError, setSignupError] = useState<string | null>(null)

  // Always prefill Admin creds when opening the login view
  useEffect(() => {
    if(view === 'login'){
      setEmail('Admin@Test.com')
      setPassword('Admin')
      setLoginError(null)
    }
    if(view === 'signup'){
      // Clear signup fields by default
      setUsername('')
      setEmail('')
      setPassword('')
    }
  }, [view])

  useEffect(() => { load() }, [])

  const trending = useMemo(() => TRENDING_MOCK[trendingPeriod], [trendingPeriod])
  const heroSlides = useMemo(() => trending.slice(0, Math.min(trending.length, 5)), [trending])
  const activeHeroIndex = heroSlides.length > 0 ? Math.min(carouselIndex, heroSlides.length - 1) : 0

  useEffect(() => {
    if(heroSlides.length === 0){
      setCarouselIndex(0)
      return
    }
    setCarouselIndex(prev => prev % heroSlides.length)
  }, [heroSlides.length])

  useEffect(() => {
    setCarouselIndex(0)
  }, [trendingPeriod])

  useEffect(() => {
    if(view !== 'app' || tab !== 'home') return
    if(heroSlides.length <= 1) return
    const handle = window.setInterval(() => {
      setCarouselIndex(prev => (prev + 1) % heroSlides.length)
    }, 6000)
    return () => window.clearInterval(handle)
  }, [view, tab, heroSlides.length])

  async function load(){
    setBusy(true)
    try{
      const [s, m, t] = await Promise.all([
        getSummary().catch(()=>null),
        getList('movie').then(d=>d.results).catch(()=>[]),
        getList('tv').then(d=>d.results).catch(()=>[]),
      ])
      setSummary(s)
      setMovies(m)
      setTv(t)
    } finally {
      setBusy(false)
    }
  }

  async function onRefresh(){
    setBusy(true)
    try{ await refresh(1); await load() } finally { setBusy(false) }
  }

  async function onSearch(ev: React.FormEvent){
    ev.preventDefault()
    if(!q.trim()) return
    setBusy(true)
    try{ const d = await search(q.trim()); setResults(d.results) } finally { setBusy(false) }
    setTab('search')
  }

  // Lazy-load accounts only when the view is opened
  useEffect(() => {
    if(view === 'accounts'){
      (async () => {
        try {
          setAccountsError(null)
          // health first to drive status chip
          const health = await getHealth().catch(()=>null)
          setBackendOnline(health?.status === 'healthy')
          const rows = await getUsers()
          setAccounts(rows)
        } catch (e) {
          setAccounts([])
          setAccountsError('Could not load accounts. Ensure the backend is running and the dev proxy is active.')
          setBackendOnline(false)
        }
      })()
    }
  }, [view])

  if(view === 'login'){
    const onSubmit = async (ev: React.FormEvent) => {
      ev.preventDefault()
      setLoginError(null)
      try{
        // Preflight: if backend is offline, fail fast with a clear message
        const health = await getHealth().catch(()=>null)
        if(!health || health.status !== 'healthy'){
          setLoginError('Backend is offline. Please start the backend and try again.')
          return
        }
        const res = await login(email.trim(), password)
        if(res.ok){
          const profile = { user: (res as any).user, email: (res as any).email }
          setCurrentUser(profile)
          if(remember){
            try {
              localStorage.setItem('currentUser', JSON.stringify(profile))
              localStorage.setItem('rememberUser', '1')
            } catch {}
          } else {
            try {
              localStorage.removeItem('currentUser')
              localStorage.removeItem('rememberUser')
            } catch {}
          }
          setView('app')
          setTab('home')
        } else {
          setLoginError(res.error || 'Invalid credentials')
        }
      }catch(e:any){
        setLoginError(e?.message || 'Unexpected login error')
      }
    }
    return (
      <div className="auth-page-wrapper">
        <div className="auth-background-decoration">
          <div className="auth-gradient-overlay"></div>
        </div>
        <div className="container auth-container">
          <div className="auth-card">
            <div className="auth-card-header">
              <div className="auth-icon">🎬</div>
              <h1 className="auth-card-title">Welcome Back</h1>
              <p className="auth-card-subtitle">Sign in to continue exploring movies and TV shows</p>
            </div>
            
            <form className="auth-form" onSubmit={onSubmit}>
              {loginError && (
                <div className="auth-error-message">
                  <span className="error-icon">⚠️</span>
                  <span>{loginError}</span>
                </div>
              )}
              
              <div className="form-group">
                <label className="form-label">
                  <span className="label-icon">📧</span>
                  Email Address
                </label>
                <input 
                  className="form-input" 
                  type="email" 
                  placeholder="Enter your email" 
                  value={email} 
                  onChange={e=>setEmail(e.target.value)} 
                  required
                />
              </div>

              <div className="form-group">
                <label className="form-label">
                  <span className="label-icon">🔒</span>
                  Password
                </label>
                <input 
                  className="form-input" 
                  type="password" 
                  placeholder="Enter your password" 
                  value={password} 
                  onChange={e=>setPassword(e.target.value)} 
                  required
                />
              </div>

              <div className="form-options">
                <label className="checkbox-label">
                  <input 
                    type="checkbox" 
                    checked={remember} 
                    onChange={e=>setRemember(e.target.checked)} 
                    className="checkbox-input"
                  />
                  <span>Remember me</span>
                </label>
              </div>

              <button className="btn-primary btn-submit" type="submit">
                <span>Sign In</span>
                <span className="btn-arrow">→</span>
              </button>
              
              <div className="auth-divider">
                <span>or</span>
              </div>
              
              <button 
                className="btn-secondary" 
                type="button" 
                onClick={()=> setView('accounts')}
              >
                View Stored Accounts
              </button>
            </form>
            
             <div className="auth-card-footer">
               <p>Don't have an account? <button className="link-button" onClick={()=>setView('signup')}>Sign up</button></p>
               <button className="back-link-button" onClick={()=>setView('app')}>← Back to app</button>
             </div>
          </div>
        </div>
      </div>
    )
  }

  if(view === 'signup'){
    const onSubmit = (ev: React.FormEvent) => {
      ev.preventDefault()
      setSignupError(null)
      ;(async () => {
        try {
          // Preflight: if backend is offline, fail fast with a clear message
          const health = await getHealth().catch(()=>null)
          if(!health || health.status !== 'healthy'){
            setSignupError('Backend is offline. Please start the backend and try again.')
            return
          }

          const e = email.trim()
          const p = password
          if(!e || !p){
            setSignupError('Please provide both email and password')
            return
          }

          const res = await signup(e, p, username.trim())
          if((res as any).ok){
            // After creating, go to login with email prefilled; keep password empty
            setEmail((res as any).email)
            setPassword('')
            setView('login')
          } else {
            setSignupError((res as any).error || 'Could not create account')
          }
        } catch(e:any){
          setSignupError(e?.message || 'Unexpected error during signup')
        }
      })()
    }
    return (
      <div className="auth-page-wrapper">
        <div className="auth-background-decoration">
          <div className="auth-gradient-overlay"></div>
        </div>
        <div className="container auth-container">
          <div className="auth-card">
            <div className="auth-card-header">
              <div className="auth-icon">🎭</div>
              <h1 className="auth-card-title">Join the Experience</h1>
              <p className="auth-card-subtitle">Create your account to start exploring movie and TV analytics</p>
            </div>
            
            <form className="auth-form" onSubmit={onSubmit}>
              {signupError && (
                <div className="auth-error-message">
                  <span className="error-icon">⚠️</span>
                  <span>{signupError}</span>
                </div>
              )}
              
              <div className="form-group">
                <label className="form-label">
                  <span className="label-icon">👤</span>
                  Username
                </label>
                <input 
                  className="form-input" 
                  type="text" 
                  placeholder="Choose a username" 
                  value={username} 
                  onChange={e=>setUsername(e.target.value)} 
                  required
                />
              </div>

              <div className="form-group">
                <label className="form-label">
                  <span className="label-icon">📧</span>
                  Email Address
                </label>
                <input 
                  className="form-input" 
                  type="email" 
                  placeholder="Enter your email" 
                  value={email} 
                  onChange={e=>setEmail(e.target.value)} 
                  required
                />
              </div>

              <div className="form-group">
                <label className="form-label">
                  <span className="label-icon">🔒</span>
                  Password
                </label>
                <input 
                  className="form-input" 
                  type="password" 
                  placeholder="Create a password" 
                  value={password} 
                  onChange={e=>setPassword(e.target.value)} 
                  required
                />
              </div>

              <button className="btn-primary btn-submit" type="submit">
                <span>Create Account</span>
                <span className="btn-arrow">→</span>
              </button>
              
              <div className="auth-divider">
                <span>or</span>
              </div>
              
              <button 
                className="btn-secondary" 
                type="button" 
                onClick={()=> setView('accounts')}
              >
                View Stored Accounts
              </button>
            </form>
            
             <div className="auth-card-footer">
               <p>Already have an account? <button className="link-button" onClick={()=> { 
                 setCurrentUser(null);
                 try { localStorage.removeItem('currentUser'); localStorage.removeItem('rememberUser') } catch {}
                 setView('login');
               }}>Sign in</button></p>
               <button className="back-link-button" onClick={()=>setView('app')}>← Back to app</button>
             </div>
          </div>
        </div>
      </div>
    )
  }

  // Auth landing page (Log in / Sign Up)
  const posterFor = (path?: string) => {
    if(!path) return undefined
    return path.startsWith('http') ? path : `https://image.tmdb.org/t/p/w185${path}`
  }

  const backdropFor = (path?: string) => {
    if(!path) return undefined
    return path.startsWith('http') ? path : `https://image.tmdb.org/t/p/w1280${path}`
  }

  if(view === 'auth'){
    return (
      <div className="auth-page-wrapper">
        <div className="auth-background-decoration">
          <div className="auth-gradient-overlay"></div>
        </div>
        <div className="auth-landing-content">
          <button className="back-link-button-top" onClick={()=>setView('app')}>← Back to App</button>
          
          <div className="auth-landing-header">
            <div className="auth-landing-icon">🎬</div>
            <h1 className="auth-landing-title">Movie &amp; TV Analytics</h1>
            <p className="auth-landing-tagline">Discover insights, track trends, and explore the world of entertainment</p>
          </div>

          <div className="auth-landing-features">
            <div className="auth-feature">
              <div className="auth-feature-icon">📊</div>
              <h3>Analytics Dashboard</h3>
              <p>View comprehensive statistics and trends</p>
            </div>
            <div className="auth-feature">
              <div className="auth-feature-icon">🎥</div>
              <h3>Movie Database</h3>
              <p>Explore thousands of movies and TV shows</p>
            </div>
            <div className="auth-feature">
              <div className="auth-feature-icon">🔍</div>
              <h3>Smart Search</h3>
              <p>Find your favorite content instantly</p>
            </div>
          </div>

          <div className="auth-landing-actions">
            <button className="btn-primary btn-landing" onClick={()=>setView('login')}>
              <span>Sign In</span>
              <span className="btn-arrow">→</span>
            </button>
            <button className="btn-secondary btn-landing" onClick={()=>setView('signup')}>
              Create Account
            </button>
          </div>
        </div>
      </div>
    )
  }

  if(view === 'accounts'){
    return (
      <div className="container auth-container">
        <h1 className="form-title">Stored Accounts</h1>
        <div style={{display:'flex', gap:12, alignItems:'center', marginBottom:12}}>
          <span className="chip" style={{background: backendOnline? '#2e7d32':'#b00020', color:'#fff'}}>
            Backend: {backendOnline===null? '—' : backendOnline? 'Online' : 'Offline'}
          </span>
          <button className="btn-outline" onClick={()=> setView('accounts')}>Reload</button>
        </div>
        <div style={{overflowX:'auto', width:'100%'}}>
          <table style={{width:'100%', borderCollapse:'collapse'}}>
            <thead>
              <tr>
                <th style={{textAlign:'left', borderBottom:'1px solid #ddd', padding:'8px'}}>User</th>
                <th style={{textAlign:'left', borderBottom:'1px solid #ddd', padding:'8px'}}>Email</th>
                <th style={{textAlign:'left', borderBottom:'1px solid #ddd', padding:'8px'}}>Password</th>
              </tr>
            </thead>
            <tbody>
              {accountsError ? (
                <tr>
                  <td colSpan={3} style={{padding:'12px', color:'#b00020'}}>{accountsError}</td>
                </tr>
              ) : accounts.length === 0 ? (
                <tr>
                  <td colSpan={3} style={{padding:'12px'}}>No accounts found.</td>
                </tr>
              ) : accounts.map((u, idx) => (
                <tr key={idx}>
                  <td style={{borderBottom:'1px solid #f0f0f0', padding:'8px'}}>{u.user}</td>
                  <td style={{borderBottom:'1px solid #f0f0f0', padding:'8px'}}>{u.email}</td>
                  <td style={{borderBottom:'1px solid #f0f0f0', padding:'8px', fontFamily:'monospace', fontSize:'11px'}}>{u.password}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="form-footer" style={{marginTop:'16px'}}>
          <button className="btn-link" onClick={()=>setView('login')}>Back</button>
        </div>
        <p style={{fontSize:'12px', color:'#666', marginTop:'8px'}}>Passwords are stored hashed for security and shown here as stored values.</p>
      </div>
    )
  }

  return (
    <div className="container">
      <header className="header">
        <div className="brand"><span>Movie & TV Analytics</span>{currentUser && <span className="chip user-chip-blue brand-user-chip">User: {currentUser.user}</span>}</div>
        <nav className="nav">
          <div className="nav-left">
            <button className={tab==='home'? 'active':''} onClick={()=>setTab('home')}>Home</button>
          </div>
          <button className={tab==='analytics'? 'active':''} onClick={()=>setTab('analytics')}>Analytics</button>
          <button className={tab==='movies'? 'active':''} onClick={()=>setTab('movies')}>Movies</button>
          <button className={tab==='tv'? 'active':''} onClick={()=>setTab('tv')}>TV</button>
        </nav>
        <form className="search" onSubmit={onSearch}>
          <input placeholder="Search TMDb…" value={q} onChange={e=>setQ(e.target.value)} />
          <button type="submit">Search</button>
          {!currentUser ? (
            <>
              <button type="button" onClick={()=>setView('login')}>Log In</button>
              <button type="button" onClick={()=>setView('signup')}>Sign Up</button>
            </>
          ) : (
            <button
              type="button"
              onClick={()=>{ 
                setCurrentUser(null);
                try { localStorage.removeItem('currentUser'); localStorage.removeItem('rememberUser') } catch {}
              }}
            >Log Out</button>
          )}
        </form>
      </header>
      {tab==='home' && (
        <section className="hero">
          <h1>Movie &amp; TV Analytics</h1>
          <p>Track trends, analyze ratings, and discover insights across your favorite movies and shows.</p>
          <div className="hero-actions">
            <button className="btn-solid btn-lg" onClick={()=>setTab('analytics')}>Get Started</button>
          </div>

          <div className="features">
            <div className="feature-card">
              <div className="feature-icon">🎬</div>
              <h3>Explore Movies</h3>
              <p>Dive into box office stats, critic reviews, and audience trends for every film.</p>
            </div>
            <div className="feature-card">
              <div className="feature-icon">📺</div>
              <h3>Track TV Shows</h3>
              <p>Analyze popularity over seasons, compare networks, and follow ratings evolution.</p>
            </div>
            <div className="feature-card">
              <div className="feature-icon">📈</div>
              <h3>View Analytics</h3>
              <p>Visualize data trends, compare genres, and uncover audience preferences.</p>
            </div>
          </div>

          <div className="home-spotlight">
            <div className="trending-hero">
              {heroSlides.length === 0 ? (
                <div className="trending-hero-empty">
                  Connect your TMDb API key to replace these placeholder tiles with live trending data.
                </div>
              ) : (
                heroSlides.map((item, idx) => {
                  const isActive = idx === activeHeroIndex
                  const backdrop = backdropFor(item.backdrop_path)
                  const overview = item.overview && item.overview.length > 220 ? `${item.overview.slice(0, 217)}…` : item.overview
                  return (
                    <div
                      key={`hero-${item.media_type}-${item.tmdb_id}`}
                      className={`trending-hero-slide ${isActive ? 'active' : ''}`}
                    >
                      <div
                        className="trending-hero-backdrop"
                        style={backdrop ? { backgroundImage: `url(${backdrop})` } : undefined}
                      />
                      <div className="trending-hero-overlay" />
                      <div className="trending-hero-content">
                        <div className="trending-hero-pill">{item.media_type === 'movie' ? 'Movie' : 'TV Series'}</div>
                        <h2>{item.title}</h2>
                        <p>{overview || 'No synopsis available just yet.'}</p>
                        <div className="trending-hero-meta">
                          <span>⭐ {item.vote_average?.toFixed?.(1) ?? '—'}</span>
                          {item.release_date && <span>📅 {item.release_date}</span>}
                          {(item.genres?.length ?? 0) > 0 && <span>{item.genres!.slice(0, 2).join(' • ')}</span>}
                        </div>
                      </div>
                    </div>
                  )
                })
              )}

              {heroSlides.length > 1 && (
                <>
                  <button
                    type="button"
                    className="trending-hero-nav prev"
                    onClick={() => setCarouselIndex(prev => (prev - 1 + heroSlides.length) % heroSlides.length)}
                    aria-label="Previous slide"
                  >
                    ‹
                  </button>
                  <button
                    type="button"
                    className="trending-hero-nav next"
                    onClick={() => setCarouselIndex(prev => (prev + 1) % heroSlides.length)}
                    aria-label="Next slide"
                  >
                    ›
                  </button>
                  <div className="trending-hero-dots">
                    {heroSlides.map((_, idx) => (
                      <button
                        type="button"
                        key={`dot-${idx}`}
                        aria-label={`Show slide ${idx + 1}`}
                        className={idx === activeHeroIndex ? 'active' : ''}
                        onClick={() => setCarouselIndex(idx)}
                      />
                    ))}
                  </div>
                </>
              )}
            </div>

            <aside className="trending-rail">
              <div className="trending-rail-header">
                <div>
                  <h3>Popular &amp; Trending</h3>
                  <p>Sample leaderboard until the TMDb API is connected.</p>
                </div>
                <div className="trending-rail-tabs">
                  {(['weekly', 'monthly', 'all'] as TrendingPeriod[]).map(period => (
                    <button
                      type="button"
                      key={period}
                      className={period === trendingPeriod ? 'active' : ''}
                      onClick={() => setTrendingPeriod(period)}
                    >
                      {period === 'weekly' ? 'Weekly' : period === 'monthly' ? 'Monthly' : 'All Time'}
                    </button>
                  ))}
                </div>
              </div>

              <div className="trending-rail-list">
                {trending.length === 0 && (
                  <div className="trending-rail-empty">
                    Add your TMDb API key to load live rankings.
                  </div>
                )}
                {trending.length > 0 && trending.map((item, index) => {
                  const poster = posterFor(item.poster_path)
                  const genres = (item.genres && item.genres.length > 0) ? item.genres.slice(0, 2).join(' • ') : '—'
                  return (
                    <div key={`leaderboard-${item.media_type}-${item.tmdb_id}`} className="trending-rail-item">
                      <div className="rank">{index + 1}</div>
                      <div className="thumb">
                        {poster ? <img src={poster} alt={item.title} loading="lazy" /> : <div className="thumb-placeholder">🎬</div>}
                      </div>
                      <div className="details">
                        <div className="title" title={item.title}>{item.title}</div>
                        <div className="meta">
                          <span>{item.media_type === 'movie' ? 'Movie' : 'TV'}</span>
                          <span>⭐ {item.vote_average?.toFixed?.(1) ?? '—'}</span>
                        </div>
                        <div className="genres">{genres}</div>
                      </div>
                    </div>
                  )
                })}
              </div>
            </aside>
          </div>

          {/* Footer removed per design update */}
        </section>
      )}


      <div className="toolbar">
        <button onClick={onRefresh} disabled={busy}>↻ Refresh (ingest 1 page)</button>
        <span className="hint">Set TMDB_API_KEY in backend .env to enable refresh/search.</span>
      </div>

      {tab==='analytics' && (
        <section>
          <div className="stats">
            <Stat label="Items" value={summary?.total_items ?? '–'} />
            <Stat label="Movies" value={summary?.movies ?? '–'} />
            <Stat label="TV" value={summary?.tv ?? '–'} />
            <Stat label="Avg Rating" value={(summary?.avg_rating ?? 0).toFixed(1)} />
          </div>

          <h3>Top Genres</h3>
          <div className="bars">
            {(summary?.top_genres ?? []).map((g:any)=> (
              <div className="bar" key={g.genre}>
                <div className="bar-label">{g.genre || '—'}</div>
                <div className="bar-track"><div style={{width: `${Math.min(100, g.count)}%`}}/></div>
                <div className="bar-value">{g.count}</div>
              </div>
            ))}
          </div>

          <h3>Languages</h3>
          <div className="chips">
            {(summary?.languages ?? []).map((l:any)=> (
              <span className="chip" key={l.language}>{l.language.toUpperCase()} · {l.count}</span>
            ))}
          </div>
        </section>
      )}

      {tab==='movies' && (
        <section>
          <h3>Top Movies</h3>
          <div className="grid">{movies.map(m => <Card key={`m-${m.tmdb_id}`} item={m} />)}</div>
        </section>
      )}

      {tab==='tv' && (
        <section>
          <h3>Top TV</h3>
          <div className="grid">{tv.map(m => <Card key={`t-${m.tmdb_id}`} item={m} />)}</div>
        </section>
      )}

      {tab==='search' && (
        <section>
          <h3>Search Results</h3>
          <div className="grid">{results.map(m => <Card key={`s-${m.media_type}-${m.tmdb_id}`} item={m} />)}</div>
        </section>
      )}

      {busy && <div className="busy">Loading…</div>}
    </div>
  )
}
