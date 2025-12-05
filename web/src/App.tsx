import { useEffect, useMemo, useRef, useState, type CSSProperties } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import './App.css'
import { getSummary, getList, refresh, search, type MediaItem, getUsers, type UserRow, getHealth, login, signup, getTrending, type TrendingItem, getNewReleases, getMovieDetail, getShowDetail, type MovieDetail, type ShowDetail, getGenres, getLanguages, createMedia, uploadImage, deleteMedia, copyMedia, updateMedia, type UpdateMediaPayload, getReviews, createReview, type Review, getUserByEmail } from './api'
type TrendingPeriod = 'weekly' | 'monthly' | 'all'
type ReleaseFilter = 'all' | 'movie' | 'tv'

const NEW_RELEASE_FETCH_LIMIT = 24
const NEW_RELEASE_PAGE_SIZE = 6
const LIST_PAGE_SIZE = 24

const languageDisplayNames =
  typeof Intl !== 'undefined' && (Intl as any).DisplayNames
    ? new (Intl as any).DisplayNames(['en'], { type: 'language' })
    : null

const formatLanguageLabel = (code?: string | null) => {
  if(!code) return null
  const normalized = code.trim().toLowerCase()
  if(!normalized) return null
  try {
    const readable = (languageDisplayNames as any)?.of?.(normalized)
    if(typeof readable === 'string' && readable.trim().length > 0) {
      return readable
    }
  } catch {}
  return normalized.toUpperCase()
}

const getImageUrl = (path?: string | null, size: 'w92' | 'w185' | 'w300' | 'w500' | 'w780' = 'w300') => {
  if(!path) return undefined
  // If it's a local path (starts with imageofmovie/), use the API endpoint
  if(path.startsWith('imageofmovie/')) {
    return `/api/images/${path.replace('imageofmovie/', '')}`
  }
  // Otherwise, treat as TMDb path
  return `https://image.tmdb.org/t/p/${size}${path}`
}

function Card({
  item, 
  onClick, 
  style,
  selectionMode = false,
  selected = false,
  onSelectChange
}:{
  item:MediaItem
  onClick?: () => void
  style?: CSSProperties
  selectionMode?: boolean
  selected?: boolean
  onSelectChange?: (selected: boolean) => void
}){
  const img = getImageUrl(item.poster_path, 'w300')
  const handleCardClick = (e: React.MouseEvent) => {
    if(selectionMode && onSelectChange){
      e.stopPropagation()
      onSelectChange(!selected)
    } else if(onClick){
      onClick()
    }
  }
  const handleCheckboxChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    e.stopPropagation()
    if(onSelectChange){
      onSelectChange(e.target.checked)
    }
  }
  return (
    <div
      className={`card${onClick && !selectionMode ? ' card-clickable' : ''}${selectionMode ? ' card-selection-mode' : ''}${selected ? ' card-selected' : ''}`}
      onClick={handleCardClick}
      style={style}
    >
      {selectionMode && (
        <div className="card-checkbox-wrapper" onClick={(e) => e.stopPropagation()}>
          <input
            type="checkbox"
            className="card-checkbox"
            checked={selected}
            onChange={handleCheckboxChange}
            onClick={(e) => e.stopPropagation()}
          />
        </div>
      )}
      <div className="card-media">
        {img ? <img src={img} alt={item.title} loading="lazy"/> : <div className="noimg">No image</div>}
      </div>
      <div className="card-body">
        <div className="chip">{item.media_type?.toUpperCase()}</div>
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

function Stat({label, value, hint}:{label:string;value:React.ReactNode;hint?:React.ReactNode}){
  return (
    <div className="stat">
      <div className="stat-value">{value}</div>
      <div className="stat-label">{label}</div>
      {hint ? <div className="stat-hint">{hint}</div> : null}
    </div>
  )
}

export default function App() {
  const navigate = useNavigate()
  const location = useLocation()
  
  // Initialize state from URL on first mount
  const getInitialTab = () => {
    const path = location.pathname
    if (path === '/analytics') return 'analytics'
    if (path === '/movies') return 'movies'
    if (path === '/tv') return 'tv'
    if (path === '/search') return 'search'
    return 'home'
  }
  
  const getInitialView = () => {
    const path = location.pathname
    if (path === '/login') return 'login'
    if (path === '/signup') return 'signup'
    if (path === '/accounts') return 'accounts'
    if (path === '/add') return 'add'
    if (path.startsWith('/movie/') || path.startsWith('/show/')) return 'detail'
    return 'app'
  }
  
  const [tab, setTab] = useState<string>(getInitialTab)
  const [view, setView] = useState<string>(getInitialView)
  const [busy, setBusy] = useState(false)
  const [summary, setSummary] = useState<any>()
  const [movies, setMovies] = useState<MediaItem[]>([])
  const [moviesPage, setMoviesPage] = useState(1)
  const [moviesTotal, setMoviesTotal] = useState(0)
  const [moviesLoading, setMoviesLoading] = useState(false)
  const [moviesSlideDirection, setMoviesSlideDirection] = useState<'left' | 'right' | 'none'>('none')
  const [moviesTransitionType, setMoviesTransitionType] = useState<'slide' | 'none'>('none')
  const [moviesViewTransition, setMoviesViewTransition] = useState<'idle' | 'entering'>('idle')
  const [moviesGenre, setMoviesGenre] = useState<string>('all')
  const [moviesLanguage, setMoviesLanguage] = useState<string>('all')
  const [moviesSort, setMoviesSort] = useState<string>('popularity')
  const [moviesPendingGenre, setMoviesPendingGenre] = useState<string>('all')
  const [moviesPendingLanguage, setMoviesPendingLanguage] = useState<string>('all')
  const [moviesPendingSort, setMoviesPendingSort] = useState<string>('popularity')
  const [tv, setTv] = useState<MediaItem[]>([])
  const [tvPage, setTvPage] = useState(1)
  const [tvTotal, setTvTotal] = useState(0)
  const [tvLoading, setTvLoading] = useState(false)
  const [tvSlideDirection, setTvSlideDirection] = useState<'left' | 'right' | 'none'>('none')
  const [tvTransitionType, setTvTransitionType] = useState<'slide' | 'none'>('none')
  const [tvViewTransition, setTvViewTransition] = useState<'idle' | 'entering'>('idle')
  const [tvGenre, setTvGenre] = useState<string>('all')
  const [tvLanguage, setTvLanguage] = useState<string>('all')
  const [tvSort, setTvSort] = useState<string>('popularity')
  const [tvPendingGenre, setTvPendingGenre] = useState<string>('all')
  const [tvPendingLanguage, setTvPendingLanguage] = useState<string>('all')
  const [tvPendingSort, setTvPendingSort] = useState<string>('popularity')
  const [availableGenres, setAvailableGenres] = useState<string[]>([])
  const [availableLanguages, setAvailableLanguages] = useState<string[]>([])
  const [q, setQ] = useState('')
  const [results, setResults] = useState<MediaItem[]>([])
  const [searchHints, setSearchHints] = useState<MediaItem[]>([])
  const [searchHintsLoading, setSearchHintsLoading] = useState(false)
  const searchHintRequestId = useRef(0)
  const [showSearchHints, setShowSearchHints] = useState(false)
  const searchBlurTimeout = useRef<ReturnType<typeof setTimeout> | null>(null)
  const [username, setUsername] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [accounts, setAccounts] = useState<UserRow[]>([])
  const [accountsError, setAccountsError] = useState<string | null>(null)
  const [backendOnline, setBackendOnline] = useState<boolean | null>(null)
  const [trendingPeriod, setTrendingPeriod] = useState<TrendingPeriod>('weekly')
  const [trending, setTrending] = useState<TrendingItem[]>([])
  const [trendingLoading, setTrendingLoading] = useState(false)
  const [trendingError, setTrendingError] = useState<string | null>(null)
  const [trendingFadeState, setTrendingFadeState] = useState<'visible' | 'fading-out' | 'fading-in'>('visible')
  const trendingRequestId = useRef(0)
  const [carouselIndex, setCarouselIndex] = useState(0)
  // Carousel is always locked to weekly trending
  const [carouselSlides, setCarouselSlides] = useState<TrendingItem[]>([])
  const [carouselLoading, setCarouselLoading] = useState(false)
  const [newReleases, setNewReleases] = useState<MediaItem[]>([])
  const [newReleasesLoading, setNewReleasesLoading] = useState(false)
  const [newReleasesError, setNewReleasesError] = useState<string | null>(null)
  const [newReleaseFilter, setNewReleaseFilter] = useState<ReleaseFilter>('all')
  const [newReleasePage, setNewReleasePage] = useState(0)
  const [newReleasesFadeState, setNewReleasesFadeState] = useState<'visible' | 'fading-out' | 'fading-in'>('visible')
  const [newReleaseSlideDirection, setNewReleaseSlideDirection] = useState<'left' | 'right' | 'none'>('none')
  const [newReleaseTransitionType, setNewReleaseTransitionType] = useState<'fade' | 'slide' | 'none'>('none')
  const newReleasesRequestId = useRef(0)
  // Remember-me support: if a profile was stored and flag set, restore it.
  const [currentUser, setCurrentUser] = useState<{user:string;email:string;user_id?:number}|null>(() => {
    try {
      const rawSession = sessionStorage.getItem('currentUser')
      if(rawSession){
        return JSON.parse(rawSession)
      }
    } catch {}
    try {
      const flag = localStorage.getItem('rememberUser') === '1'
      if(!flag) return null
      const raw = localStorage.getItem('currentUser')
      return raw? JSON.parse(raw) : null
    } catch { return null }
    return null
  })
  const [remember, setRemember] = useState<boolean>(() => localStorage.getItem('rememberUser') === '1')
  const [loginError, setLoginError] = useState<string | null>(null)
  const [signupError, setSignupError] = useState<string | null>(null)
  const [mobileSearchOpen, setMobileSearchOpen] = useState(false)
  const searchInputRef = useRef<HTMLInputElement | null>(null)
  const accountMenuRef = useRef<HTMLDivElement | null>(null)
  const detailRequestId = useRef(0)
  const [accountMenuOpen, setAccountMenuOpen] = useState(false)
  const [detailData, setDetailData] = useState<MovieDetail | ShowDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [detailEditMode, setDetailEditMode] = useState(false)
  const [editTitle, setEditTitle] = useState('')
  const [editOverview, setEditOverview] = useState('')
  const [editLanguage, setEditLanguage] = useState('')
  const [editYear, setEditYear] = useState('')
  const [editTmdbScore, setEditTmdbScore] = useState('')
  const [editPopularity, setEditPopularity] = useState('')
  const [editGenre, setEditGenre] = useState('')
  const [editSaving, setEditSaving] = useState(false)
  const [reviews, setReviews] = useState<Review[]>([])
  const [reviewsLoading, setReviewsLoading] = useState(false)
  const [reviewText, setReviewText] = useState('')
  const [reviewSubmitting, setReviewSubmitting] = useState(false)
  const [moviesAnimationKey, setMoviesAnimationKey] = useState(0)
  const [tvAnimationKey, setTvAnimationKey] = useState(0)
  const moviesRequestId = useRef(0)
  const tvRequestId = useRef(0)
  const [moviesReady, setMoviesReady] = useState(false)
  const [tvReady, setTvReady] = useState(false)
  const [moviesSelectionMode, setMoviesSelectionMode] = useState(false)
  const [moviesSelected, setMoviesSelected] = useState<Set<number>>(new Set())
  const [moviesDeleting, setMoviesDeleting] = useState(false)
  const [moviesCopying, setMoviesCopying] = useState(false)
  const [tvSelectionMode, setTvSelectionMode] = useState(false)
  const [tvSelected, setTvSelected] = useState<Set<number>>(new Set())
  const [tvDeleting, setTvDeleting] = useState(false)
  const [tvCopying, setTvCopying] = useState(false)
  const [addMediaType, setAddMediaType] = useState<'movie' | 'tv'>('movie')
  const [addTitle, setAddTitle] = useState('')
  const [addOverview, setAddOverview] = useState('')
  const [addLanguage, setAddLanguage] = useState('')
  const [addYear, setAddYear] = useState('')
  const [addTmdbScore, setAddTmdbScore] = useState('')
  const [addPopularity, setAddPopularity] = useState('')
  const [addPosterPath, setAddPosterPath] = useState('')
  const [addGenre, setAddGenre] = useState('')
  const [addSubmitting, setAddSubmitting] = useState(false)
  const [addError, setAddError] = useState<string | null>(null)
  const [addPosterPreview, setAddPosterPreview] = useState<string | null>(null)
  const [addPosterFile, setAddPosterFile] = useState<File | null>(null)
  const addPosterInputRef = useRef<HTMLInputElement | null>(null)
  const primaryNav = [
    { id: 'analytics', label: 'Analytics' },
    { id: 'movies', label: 'Movies' },
    { id: 'tv', label: 'TV' },
  ]
  const canSubmitSearch = q.trim().length > 0

  // Sync URL to state on mount and location change
  useEffect(() => {
    const path = location.pathname
    if (path === '/' || path === '/home') {
      setView('app')
      setTab('home')
    } else if (path === '/analytics') {
      setView('app')
      setTab('analytics')
    } else if (path === '/movies') {
      setView('app')
      setTab('movies')
    } else if (path === '/tv') {
      setView('app')
      setTab('tv')
    } else if (path === '/search') {
      setView('app')
      setTab('search')
    } else if (path === '/login') {
      setView('login')
    } else if (path === '/signup') {
      setView('signup')
    } else if (path === '/accounts') {
      setView('accounts')
    } else if (path === '/add') {
      setView('add')
    } else if (path.startsWith('/movie/') || path.startsWith('/show/')) {
      setView('detail')
    }
  }, [location.pathname])

  // Helper to navigate with state sync
  const resetMoviesState = () => {
    moviesRequestId.current++
    setMoviesGenre('all')
    setMoviesLanguage('all')
    setMoviesSort('popularity')
    setMoviesPendingGenre('all')
    setMoviesPendingLanguage('all')
    setMoviesPendingSort('popularity')
    setMovies([])
    setMoviesTotal(0)
    setMoviesPage(1)
    setMoviesAnimationKey(prev => prev + 1)
    setMoviesReady(false)
  }

  const resetTvState = () => {
    tvRequestId.current++
    setTvGenre('all')
    setTvLanguage('all')
    setTvSort('popularity')
    setTvPendingGenre('all')
    setTvPendingLanguage('all')
    setTvPendingSort('popularity')
    setTv([])
    setTvTotal(0)
    setTvPage(1)
    setTvAnimationKey(prev => prev + 1)
    setTvReady(false)
  }

  const navigateToTab = (newTab: string) => {
    const targetPath = `/${newTab === 'home' ? '' : newTab}`
    if(newTab !== tab){
      if(newTab === 'movies'){
        resetMoviesState()
        setMoviesSelectionMode(false)
        setMoviesSelected(new Set())
        loadMovies(1)
      } else if(newTab === 'tv'){
        resetTvState()
        setTvSelectionMode(false)
        setTvSelected(new Set())
        loadTv(1)
      }
    }
    setTab(newTab)
    setView('app')
    navigate(targetPath)
  }

  const navigateToView = (newView: string) => {
    setView(newView)
    if(newView === 'app') {
      navigate('/')
    } else if (newView === 'add') {
      navigate('/add')
    } else {
      navigate(`/${newView}`)
    }
  }

  const handleLogout = () => {
    setAccountMenuOpen(false)
    setCurrentUser(null)
    try {
      sessionStorage.removeItem('currentUser')
      localStorage.removeItem('currentUser')
      localStorage.removeItem('rememberUser')
    } catch {}
  }

  const avatarInitials = useMemo(() => {
    const name = currentUser?.user ?? ''
    if(!name.trim()) return ''
    const parts = name.trim().split(/\s+/).slice(0, 2)
    const letters = parts.map(part => part.charAt(0)?.toUpperCase() ?? '').join('')
    return letters || ''
  }, [currentUser?.user])

  const renderAccountControls = () => {
    if(!currentUser){
      return (
        <button
          type="button"
          className="btn-outline header-auth"
          onClick={()=>navigateToView('login')}
        >
          Log In
        </button>
      )
    }
    const displayName = currentUser.user?.trim() || 'Signed in'
    const displayEmail = currentUser.email || ''
    return (
      <div className={`header-account${accountMenuOpen ? ' open' : ''}`} ref={accountMenuRef}>
        <button
          type="button"
          className="avatar-button"
          aria-haspopup="menu"
          aria-expanded={accountMenuOpen}
          onClick={()=>setAccountMenuOpen(prev => !prev)}
        >
          <span className="visually-hidden">Open account options</span>
          <span className="avatar-circle" aria-hidden="true">
            {avatarInitials ? (
              <span className="avatar-initials">{avatarInitials}</span>
            ) : (
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" role="presentation">
                <path
                  d="M12 12.75a4.25 4.25 0 1 0 0-8.5 4.25 4.25 0 0 0 0 8.5Z"
                  stroke="currentColor"
                  strokeWidth="1.6"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
                <path
                  d="M18.5 19.5c0-3.59-2.91-6.5-6.5-6.5s-6.5 2.91-6.5 6.5"
                  stroke="currentColor"
                  strokeWidth="1.6"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            )}
          </span>
        </button>
        {accountMenuOpen && (
          <div className="account-menu" role="menu">
            <div className="account-menu-header">
              <span className="account-menu-name">{displayName}</span>
              <span className="account-menu-email">{displayEmail}</span>
            </div>
            <button type="button" className="account-menu-item" role="menuitem" disabled>
              <span className="menu-icon" aria-hidden="true">👤</span>
              <span className="menu-content">
                <span className="menu-title">Profile</span>
                <span className="menu-subtitle">Coming soon</span>
              </span>
            </button>
            <button type="button" className="account-menu-item" role="menuitem" disabled>
              <span className="menu-icon" aria-hidden="true">⚙️</span>
              <span className="menu-content">
                <span className="menu-title">User Settings</span>
                <span className="menu-subtitle">Coming soon</span>
              </span>
            </button>
            <button
              type="button"
              className="account-menu-item"
              role="menuitem"
              onClick={() => {
                setAccountMenuOpen(false)
                navigateToView('add')
              }}
            >
              <span className="menu-icon" aria-hidden="true">🎬</span>
              <span className="menu-content">
                <span className="menu-title">Add Movie/TV</span>
                <span className="menu-subtitle">Go to catalog to add a title</span>
              </span>
            </button>
            <div className="account-menu-separator" role="none" />
            <button
              type="button"
              className="account-menu-item danger"
              role="menuitem"
              onClick={handleLogout}
            >
              <span className="menu-icon" aria-hidden="true">🚪</span>
              <span className="menu-content">
                <span className="menu-title">Log Out</span>
                <span className="menu-subtitle">Sign out of this session</span>
              </span>
            </button>
          </div>
        )}
      </div>
    )
  }

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

  useEffect(() => {
    if(!mobileSearchOpen) return
    searchInputRef.current?.focus()
  }, [mobileSearchOpen])

  useEffect(() => {
    const term = q.trim()
    if(term.length < 2){
      setSearchHints([])
      setSearchHintsLoading(false)
      return
    }
    const requestId = ++searchHintRequestId.current
    setSearchHintsLoading(true)
    ;(async () => {
      try{
        const data = await search(term, 1)
        if(requestId === searchHintRequestId.current){
          setSearchHints((data?.results ?? []).slice(0, 6))
        }
      } catch (err){
        if(requestId === searchHintRequestId.current){
          setSearchHints([])
        }
        console.error('Search preview failed', err)
      } finally {
        if(requestId === searchHintRequestId.current){
          setSearchHintsLoading(false)
        }
      }
    })()
  }, [q])

  useEffect(() => {
    return () => {
      if(searchBlurTimeout.current){
        clearTimeout(searchBlurTimeout.current)
      }
    }
  }, [])

  useEffect(() => {
    if(typeof window === 'undefined') return
    const mq = window.matchMedia('(min-width: 640px)')
    const handler = () => setMobileSearchOpen(false)
    handler()
    if(mq.addEventListener){
      mq.addEventListener('change', handler)
    } else {
      mq.addListener(handler)
    }
    return () => {
      if(mq.removeEventListener){
        mq.removeEventListener('change', handler)
      } else {
        mq.removeListener(handler)
      }
    }
  }, [])

  useEffect(() => {
    if(view !== 'app'){
      setMobileSearchOpen(false)
    }
  }, [view])

  useEffect(() => {
    if(!currentUser){
      setAccountMenuOpen(false)
    }
  }, [currentUser])

  useEffect(() => {
    if(!accountMenuOpen) return
    const handlePointerDown = (event: MouseEvent | TouchEvent) => {
      const target = event.target as Node | null
      if(!target) return
      if(accountMenuRef.current && accountMenuRef.current.contains(target)) return
      setAccountMenuOpen(false)
    }
    document.addEventListener('mousedown', handlePointerDown)
    document.addEventListener('touchstart', handlePointerDown)
    return () => {
      document.removeEventListener('mousedown', handlePointerDown)
      document.removeEventListener('touchstart', handlePointerDown)
    }
  }, [accountMenuOpen])

  useEffect(() => {
    if(!accountMenuOpen) return
    const handleKeyDown = (event: KeyboardEvent) => {
      if(event.key === 'Escape'){
        setAccountMenuOpen(false)
      }
    }
    document.addEventListener('keydown', handleKeyDown)
    return () => {
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [accountMenuOpen])

  useEffect(() => {
    setAccountMenuOpen(false)
  }, [location.pathname])

  useEffect(() => {
    if(tab === 'movies'){
      setMoviesAnimationKey(prev => prev + 1)
    } else if(tab === 'tv'){
      setTvAnimationKey(prev => prev + 1)
    }
  }, [tab])

  useEffect(() => {
    load()
    // Load genres and languages
    ;(async () => {
      try {
        const [genresData, languagesData] = await Promise.all([getGenres(), getLanguages()])
        setAvailableGenres(genresData.genres || [])
        setAvailableLanguages(languagesData.languages || [])
      } catch (err) {
        console.error('Failed to load genres/languages', err)
      }
    })()
  }, [])

  // Reload movies when filters change
  useEffect(() => {
    if (tab === 'movies') {
      loadMovies(1)
      setMoviesPage(1)
    }
  }, [moviesGenre, moviesLanguage, moviesSort])

  // Reload TV when filters change
  useEffect(() => {
    if (tab === 'tv') {
      loadTv(1)
      setTvPage(1)
    }
  }, [tvGenre, tvLanguage, tvSort])

  useEffect(() => {
    loadNewReleases()
  }, [newReleaseFilter])

  // Load carousel data once (always weekly, top 5)
  useEffect(() => {
    let cancelled = false
    setCarouselLoading(true)
    ;(async () => {
      try {
        const results = await getTrending('weekly', 5)
        if (!cancelled) {
          setCarouselSlides(results)
          setCarouselIndex(0)
        }
      } catch (err) {
        if (!cancelled) {
          setCarouselSlides([])
        }
      } finally {
        if (!cancelled) {
          setCarouselLoading(false)
        }
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  // Load right-rail trending based on selected period
  useEffect(() => {
    const requestId = ++trendingRequestId.current
    const hasExistingContent = trending.length > 0
    
    setTrendingLoading(true)
    setTrendingError(null)
    
    // If we have existing content, fade it out first
    if (hasExistingContent) {
      setTrendingFadeState('fading-out')
    }
    
    ;(async () => {
      try {
        const results = await getTrending(trendingPeriod, 10)
        // Only update if this is still the latest request
        if (requestId === trendingRequestId.current) {
          // Wait for fade-out to complete before updating content
          const fadeDelay = hasExistingContent ? 200 : 0
          setTimeout(() => {
            if (requestId === trendingRequestId.current) {
              setTrending(results)
              setTrendingLoading(false)
              setTrendingFadeState('fading-in')
              // After fade-in completes, set to visible
              setTimeout(() => {
                if (requestId === trendingRequestId.current) {
                  setTrendingFadeState('visible')
                }
              }, 300)
            }
          }, fadeDelay)
        }
      } catch (err) {
        // Only update if this is still the latest request
        if (requestId === trendingRequestId.current) {
          const fadeDelay = hasExistingContent ? 200 : 0
          setTimeout(() => {
            if (requestId === trendingRequestId.current) {
              setTrending([])
              setTrendingError('Trending data unavailable. Try running the TMDb ETL loader.')
              setTrendingLoading(false)
              setTrendingFadeState('fading-in')
              setTimeout(() => {
                if (requestId === trendingRequestId.current) {
                  setTrendingFadeState('visible')
                }
              }, 300)
            }
          }, fadeDelay)
        }
      }
    })()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [trendingPeriod])

  const heroSlides = useMemo(
    () => carouselSlides.filter(item => item.backdrop_url || item.poster_url),
    [carouselSlides]
  )
  const newReleaseTotalPages = useMemo(() => {
    if(newReleases.length === 0) return 0
    return Math.ceil(newReleases.length / NEW_RELEASE_PAGE_SIZE)
  }, [newReleases.length])

  const displayedNewReleases = useMemo(() => {
    if(newReleases.length === 0) return []
    const clampedPage = Math.min(newReleasePage, Math.max(0, newReleaseTotalPages - 1))
    const start = clampedPage * NEW_RELEASE_PAGE_SIZE
    const end = start + NEW_RELEASE_PAGE_SIZE
    return newReleases.slice(start, end)
  }, [newReleases, newReleasePage, newReleaseTotalPages])


  useEffect(() => {
    if(newReleases.length === 0) {
      if(newReleasePage !== 0){
        setNewReleasePage(0)
      }
      return
    }
    if(newReleasePage > Math.max(0, newReleaseTotalPages - 1)){
      setNewReleasePage(Math.max(0, newReleaseTotalPages - 1))
    }
  }, [newReleases.length, newReleasePage, newReleaseTotalPages])

  // Reset slide direction and transition type after animation completes
  useEffect(() => {
    if (newReleaseSlideDirection !== 'none') {
      const timer = setTimeout(() => {
        setNewReleaseSlideDirection('none')
        setNewReleaseTransitionType('none')
      }, 400) // Match animation duration
      return () => clearTimeout(timer)
    }
  }, [newReleaseSlideDirection])

  useEffect(() => {
    if (moviesSlideDirection !== 'none') {
      const timer = setTimeout(() => {
        setMoviesSlideDirection('none')
        setMoviesTransitionType('none')
      }, 400)
      return () => clearTimeout(timer)
    }
  }, [moviesSlideDirection])

  useEffect(() => {
    if (tvSlideDirection !== 'none') {
      const timer = setTimeout(() => {
        setTvSlideDirection('none')
        setTvTransitionType('none')
      }, 400)
      return () => clearTimeout(timer)
    }
  }, [tvSlideDirection])

  // Reset fade state and transition type after fade completes
  useEffect(() => {
    if (newReleasesFadeState === 'visible' && newReleaseTransitionType === 'fade') {
      const timer = setTimeout(() => {
        setNewReleaseTransitionType('none')
      }, 50) // Small delay to ensure animation completes
      return () => clearTimeout(timer)
    }
  }, [newReleasesFadeState, newReleaseTransitionType])

  useEffect(() => {
    if (tab === 'movies' && movies.length > 0) {
      setMoviesViewTransition('entering')
      const timer = window.setTimeout(() => setMoviesViewTransition('idle'), 320)
      return () => window.clearTimeout(timer)
    }
  }, [tab, movies.length])

  useEffect(() => {
    if (tab === 'tv' && tv.length > 0) {
      setTvViewTransition('entering')
      const timer = window.setTimeout(() => setTvViewTransition('idle'), 320)
      return () => window.clearTimeout(timer)
    }
  }, [tab, tv.length])

  const activeHeroIndex = heroSlides.length > 0 ? Math.min(carouselIndex, heroSlides.length - 1) : 0

  useEffect(() => {
    if(heroSlides.length === 0){
      setCarouselIndex(0)
      return
    }
    setCarouselIndex(prev => prev % heroSlides.length)
  }, [heroSlides.length])

  useEffect(() => {
    if(view !== 'app' || tab !== 'home') return
    if(heroSlides.length <= 1) return
    const handle = window.setInterval(() => {
      setCarouselIndex(prev => (prev + 1) % heroSlides.length)
    }, 6000)
    return () => window.clearInterval(handle)
  }, [view, tab, heroSlides.length])

  async function loadMovies(page = 1, overrides?: { genre?: string; language?: string; sort?: string }){
    const targetPage = Math.max(1, page)
    const nextGenre = overrides?.genre ?? moviesGenre
    const nextLanguage = overrides?.language ?? moviesLanguage
    const nextSort = overrides?.sort ?? moviesSort
    const requestId = ++moviesRequestId.current
    setMoviesReady(false)
    setMoviesLoading(true)
    try{
      const data = await getList('movie', nextSort, targetPage, LIST_PAGE_SIZE, nextGenre, nextLanguage)
      if(requestId !== moviesRequestId.current) return
      setMoviesGenre(nextGenre)
      setMoviesLanguage(nextLanguage)
      setMoviesSort(nextSort)
      const results = data?.results ?? []
      const total = typeof data?.total === 'number' ? data.total : results.length
      const resolvedPage = typeof data?.page === 'number' ? data.page : targetPage
      setMovies(results)
      setMoviesTotal(total)
      setMoviesPage(Math.max(1, resolvedPage))
      setMoviesReady(true)
    } catch (err) {
      console.error('Failed to load movies', err)
      if(requestId !== moviesRequestId.current) return
      setMovies([])
      setMoviesTotal(0)
      setMoviesPage(Math.max(1, targetPage))
      setMoviesReady(true)
    } finally {
      if(requestId === moviesRequestId.current){
        setMoviesLoading(false)
      }
    }
  }

  async function loadTv(page = 1, overrides?: { genre?: string; language?: string; sort?: string }){
    const targetPage = Math.max(1, page)
    const nextGenre = overrides?.genre ?? tvGenre
    const nextLanguage = overrides?.language ?? tvLanguage
    const nextSort = overrides?.sort ?? tvSort
    const requestId = ++tvRequestId.current
    setTvReady(false)
    setTvLoading(true)
    try{
      const data = await getList('tv', nextSort, targetPage, LIST_PAGE_SIZE, nextGenre, nextLanguage)
      if(requestId !== tvRequestId.current) return
      setTvGenre(nextGenre)
      setTvLanguage(nextLanguage)
      setTvSort(nextSort)
      const results = data?.results ?? []
      const total = typeof data?.total === 'number' ? data.total : results.length
      const resolvedPage = typeof data?.page === 'number' ? data.page : targetPage
      setTv(results)
      setTvTotal(total)
      setTvPage(Math.max(1, resolvedPage))
      setTvReady(true)
    } catch (err) {
      console.error('Failed to load tv shows', err)
      if(requestId !== tvRequestId.current) return
      setTv([])
      setTvTotal(0)
      setTvPage(Math.max(1, targetPage))
      setTvReady(true)
    } finally {
      if(requestId === tvRequestId.current){
        setTvLoading(false)
      }
    }
  }

  const canNavigateMoviesPrev = moviesPage > 1
  const canNavigateMoviesNext =
    moviesTotal !== 0 && moviesPage * LIST_PAGE_SIZE < moviesTotal
  const canNavigateTvPrev = tvPage > 1
  const canNavigateTvNext =
    tvTotal !== 0 && tvPage * LIST_PAGE_SIZE < tvTotal

  const goToPreviousMovies = () => {
    if (moviesLoading || !canNavigateMoviesPrev) return
    setMoviesTransitionType('slide')
    setMoviesSlideDirection('right')
    loadMovies(moviesPage - 1)
  }

  const goToNextMovies = () => {
    if (moviesLoading || !canNavigateMoviesNext) return
    setMoviesTransitionType('slide')
    setMoviesSlideDirection('left')
    loadMovies(moviesPage + 1)
  }

  const goToPreviousTv = () => {
    if (tvLoading || !canNavigateTvPrev) return
    setTvTransitionType('slide')
    setTvSlideDirection('right')
    loadTv(tvPage - 1)
  }

  const goToNextTv = () => {
    if (tvLoading || !canNavigateTvNext) return
    setTvTransitionType('slide')
    setTvSlideDirection('left')
    loadTv(tvPage + 1)
  }

  const moviesFiltersDirty = moviesPendingGenre !== moviesGenre || moviesPendingLanguage !== moviesLanguage || moviesPendingSort !== moviesSort
  const tvFiltersDirty = tvPendingGenre !== tvGenre || tvPendingLanguage !== tvLanguage || tvPendingSort !== tvSort

  const analyticsSnapshot = useMemo(() => {
    const totalItems = summary?.total_items ?? 0
    const totalMovies = summary?.movies ?? 0
    const totalTvShows = summary?.tv ?? 0
    const avgRating = summary?.avg_rating ?? 0
    const topGenres = (summary?.top_genres ?? []).filter(Boolean)
    const languages = (summary?.languages ?? []).filter(Boolean)
    const languageCount = languages.length
    const movieShare = totalItems > 0 ? Math.round((totalMovies / totalItems) * 100) : 0
    const tvShare = totalItems > 0 ? Math.round((totalTvShows / totalItems) * 100) : 0
    const otherShare = Math.max(0, 100 - movieShare - tvShare)
    const highestGenreCount = topGenres.reduce((max: number, item: any) => {
      const value = typeof item?.count === 'number' ? item.count : 0
      return value > max ? value : max
    }, 0)

    return {
      totalItems,
      totalMovies,
      totalTvShows,
      avgRating,
      topGenres,
      languages,
      languageCount,
      movieShare,
      tvShare,
      otherShare,
      highlightGenre: topGenres[0]?.genre ?? null,
      highlightGenreCount: topGenres[0]?.count ?? 0,
      highlightLanguage: languages[0]?.language ?? null,
      hasContent: totalItems > 0 || topGenres.length > 0 || languages.length > 0,
      highestGenreCount: highestGenreCount > 0 ? highestGenreCount : 1,
    }
  }, [summary])

  const applyMoviesFilters = () => {
    const nextGenre = moviesPendingGenre
    const nextLanguage = moviesPendingLanguage
    const nextSort = moviesPendingSort
    setMoviesPage(1)
    setMoviesAnimationKey(prev => prev + 1)
    loadMovies(1, { genre: nextGenre, language: nextLanguage, sort: nextSort })
  }

  const applyTvFilters = () => {
    const nextGenre = tvPendingGenre
    const nextLanguage = tvPendingLanguage
    const nextSort = tvPendingSort
    setTvPage(1)
    setTvAnimationKey(prev => prev + 1)
    loadTv(1, { genre: nextGenre, language: nextLanguage, sort: nextSort })
  }

  async function load(){
    setBusy(true)
    try{
      const summary = await getSummary().catch(()=>null)
      setSummary(summary)
      await Promise.all([
        loadMovies(1),
        loadTv(1),
      ])
    } finally {
      setBusy(false)
    }
  }

  async function loadNewReleases(limit = NEW_RELEASE_FETCH_LIMIT, filter: ReleaseFilter = newReleaseFilter){
    const requestId = ++newReleasesRequestId.current
    const hasExistingContent = newReleases.length > 0
    
    setNewReleasesLoading(true)
    setNewReleasesError(null)
    
    // If we have existing content, fade it out first
    if (hasExistingContent) {
      setNewReleaseTransitionType('fade')
      setNewReleasesFadeState('fading-out')
    }
    
    try {
      const items = await getNewReleases(limit, filter)
      
      // Only update if this is still the latest request
      if (requestId === newReleasesRequestId.current) {
        const normalized = items.map(item => {
          const base: any = { ...item }
          if(typeof base.id !== 'number' && typeof base.item_id === 'number'){
            base.id = base.item_id
          }
          if(!base.poster_path && base.poster_url){
            base.poster_path = base.poster_url
          }
          if(base.media_type === 'show'){
            base.media_type = 'tv'
          }
          return base as MediaItem
        })
        const sorted = normalized
          .slice()
          .sort((a, b) => {
            const aTime = a.release_date ? Date.parse(a.release_date) : 0
            const bTime = b.release_date ? Date.parse(b.release_date) : 0
            return bTime - aTime
          })
        
        // Wait for fade-out to complete before updating content
        const fadeDelay = hasExistingContent ? 200 : 0
        setTimeout(() => {
          if (requestId === newReleasesRequestId.current) {
            setNewReleases(sorted)
            setNewReleasePage(0)
            setNewReleasesLoading(false)
            setNewReleasesFadeState('fading-in')
            // After fade-in completes, set to visible
            setTimeout(() => {
              if (requestId === newReleasesRequestId.current) {
                setNewReleasesFadeState('visible')
              }
            }, 300)
          }
        }, fadeDelay)
      }
    } catch (err) {
      console.error('Failed to load new releases', err)
      
      // Only update if this is still the latest request
      if (requestId === newReleasesRequestId.current) {
        const fadeDelay = hasExistingContent ? 200 : 0
        setTimeout(() => {
          if (requestId === newReleasesRequestId.current) {
            setNewReleases([])
            setNewReleasesError('Unable to load new releases at the moment.')
            setNewReleasesLoading(false)
            setNewReleasesFadeState('fading-in')
            setTimeout(() => {
              if (requestId === newReleasesRequestId.current) {
                setNewReleasesFadeState('visible')
              }
            }, 300)
          }
        }, fadeDelay)
      }
    }
  }

  async function onRefresh(){
    setBusy(true)
    try{
      await refresh(1)
      await Promise.all([load(), loadNewReleases(undefined, newReleaseFilter)])
    } finally {
      setBusy(false)
    }
  }

  async function onSearch(ev: React.FormEvent){
    ev.preventDefault()
    if(!q.trim()) return
    setBusy(true)
    try{ const d = await search(q.trim()); setResults(d.results) } finally { setBusy(false) }
    navigateToTab('search')
    setMobileSearchOpen(false)
  }

  async function navigateToDetail(mediaType: 'movie' | 'tv', id?: number){
    const targetId = typeof id === 'number' ? id : Number(id)
    setDetailError(null)
    setDetailData(null)
    if(!Number.isFinite(targetId) || targetId <= 0){
      setDetailLoading(false)
      setView('detail')
      navigate(`/${mediaType}/0`)
      setDetailError('Details unavailable for this title.')
      return
    }
    const safeId = targetId as number
    setDetailLoading(true)
    setView('detail')
    navigate(`/${mediaType}/${safeId}`)
  }

  const trimmedSearch = q.trim()
  const shouldShowSearchDropdown = showSearchHints && trimmedSearch.length >= 2

  const handleSearchFocus = () => {
    if(searchBlurTimeout.current){
      clearTimeout(searchBlurTimeout.current)
    }
    setShowSearchHints(true)
  }

  const handleSearchBlur = () => {
    if(searchBlurTimeout.current){
      clearTimeout(searchBlurTimeout.current)
    }
    searchBlurTimeout.current = window.setTimeout(() => {
      setShowSearchHints(false)
    }, 180)
  }

  const handleSuggestionSelect = (item: MediaItem) => {
    const targetId = item.id ?? item.tmdb_id
    if(!targetId){
      return
    }
    setQ(item.title ?? q)
    setShowSearchHints(false)
    setMobileSearchOpen(false)
    navigateToDetail(item.media_type, targetId)
  }

  const renderSearchSuggestions = () => {
    if(!shouldShowSearchDropdown){
      return null
    }
    return (
      <div className="search-suggestions" onMouseDown={e => e.preventDefault()}>
        {searchHintsLoading && (
          <div className="search-suggestion-row search-suggestion-loading">
            <span className="search-suggestion-spinner" aria-hidden="true" />
            <span>Searching “{trimmedSearch}”…</span>
          </div>
        )}
        {!searchHintsLoading && searchHints.length === 0 && (
          <div className="search-suggestion-row search-suggestion-empty">
            No matches found for “{trimmedSearch}”.
          </div>
        )}
        {!searchHintsLoading && searchHints.map(item => (
          <button
            type="button"
            key={`${item.media_type}-${item.id ?? item.tmdb_id}`}
            className="search-suggestion-item"
            onClick={() => handleSuggestionSelect(item)}
          >
            <div className="search-suggestion-thumb">
              {item.poster_path ? (
                <img
                  src={getImageUrl(item.poster_path, 'w92')}
                  alt=""
                  loading="lazy"
                />
              ) : (
                <span role="presentation">🎬</span>
              )}
            </div>
            <div className="search-suggestion-body">
              <div className="search-suggestion-title">
                {item.title}
              </div>
              <div className="search-suggestion-meta">
                <span className="search-suggestion-type">{item.media_type === 'tv' ? 'TV' : 'Movie'}</span>
                {item.vote_average ? <span>⭐ {item.vote_average.toFixed(1)}</span> : null}
                {item.release_date ? <span>{item.release_date}</span> : null}
              </div>
            </div>
          </button>
        ))}
      </div>
    )
  }

  const renderSearchField = () => (
    <div className={`header-search-shell ${shouldShowSearchDropdown ? 'open' : ''}`}>
      <form className={`header-search ${mobileSearchOpen ? 'show' : ''}`} onSubmit={onSearch}>
        <span className="search-icon" aria-hidden="true">🔍</span>
        <input
          ref={searchInputRef}
          placeholder="Search TMDb…"
          value={q}
          onChange={e=>setQ(e.target.value)}
          onFocus={handleSearchFocus}
          onBlur={handleSearchBlur}
          autoComplete="off"
        />
        <button type="submit" className="search-submit" disabled={!canSubmitSearch}>Search</button>
      </form>
      {renderSearchSuggestions()}
    </div>
  )

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

  useEffect(() => {
    if(view !== 'detail'){
      setDetailLoading(false)
      return
    }
    const segments = location.pathname.split('/').filter(Boolean)
    if(segments.length < 2){
      setDetailError('Details unavailable for this route.')
      setDetailLoading(false)
      setDetailData(null)
      return
    }
    const resource = segments[0]
    const idSegment = segments[1]
    let mediaType: 'movie' | 'tv'
    if(resource === 'movie'){
      mediaType = 'movie'
    } else if(resource === 'tv' || resource === 'show'){
      mediaType = 'tv'
    } else {
      setDetailError('Unknown content type.')
      setDetailLoading(false)
      setDetailData(null)
      return
    }
    const numericId = Number.parseInt(idSegment, 10)
    if(!Number.isFinite(numericId) || numericId <= 0){
      setDetailError('Invalid title identifier.')
      setDetailLoading(false)
      setDetailData(null)
      return
    }

    const requestId = ++detailRequestId.current
    setDetailLoading(true)
    setDetailError(null)
    setDetailData(null)

    ;(async () => {
      try {
        if(mediaType === 'movie'){
          const data = await getMovieDetail(numericId)
          if(requestId === detailRequestId.current){
            setDetailData({ ...data, media_type: 'movie' })
            // Initialize edit fields
            setEditTitle(data.title || '')
            setEditOverview(data.overview || '')
            setEditLanguage(data.original_language || '')
            setEditYear(data.release_year?.toString() || '')
            setEditTmdbScore(data.vote_average?.toString() || '')
            setEditPopularity(data.popularity?.toString() || '')
            // Join all genres with comma and space
            setEditGenre(data.genres?.join(', ') || '')
            setDetailEditMode(false)
            // Load reviews
            setReviewsLoading(true)
            const reviewsData = await getReviews('movie', numericId)
            if(requestId === detailRequestId.current){
              setReviews(reviewsData.reviews || [])
              setReviewsLoading(false)
            }
          }
        } else {
          const data = await getShowDetail(numericId)
          if(requestId === detailRequestId.current){
            setDetailData({ ...data, media_type: 'tv' })
            // Initialize edit fields
            setEditTitle(data.title || '')
            setEditOverview(data.overview || '')
            setEditLanguage(data.original_language || '')
            const yearMatch = data.first_air_date?.match(/^(\d{4})/)
            setEditYear(yearMatch ? yearMatch[1] : '')
            setEditTmdbScore(data.vote_average?.toString() || '')
            setEditPopularity(data.popularity?.toString() || '')
            // Join all genres with comma and space
            setEditGenre(data.genres?.join(', ') || '')
            setDetailEditMode(false)
            // Load reviews
            setReviewsLoading(true)
            const reviewsData = await getReviews('show', numericId)
            if(requestId === detailRequestId.current){
              setReviews(reviewsData.reviews || [])
              setReviewsLoading(false)
            }
          }
        }
      } catch (err) {
        console.error('Failed to load detail', err)
        if(requestId === detailRequestId.current){
          setDetailError('Failed to load details. Please try again.')
          setDetailData(null)
        }
      } finally {
        if(requestId === detailRequestId.current){
          setDetailLoading(false)
        }
      }
    })()
  }, [view, location.pathname])

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
          const profile = { user: (res as any).user, email: (res as any).email, user_id: (res as any).user_id }
          setCurrentUser(profile)
          try {
            sessionStorage.setItem('currentUser', JSON.stringify(profile))
          } catch {}
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
          navigateToTab('home')
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
                onClick={()=> navigateToView('accounts')}
              >
                View Stored Accounts
              </button>
            </form>
            
             <div className="auth-card-footer">
               <p>Don't have an account? <button className="link-button" onClick={()=>navigateToView('signup')}>Sign up</button></p>
               <button className="back-link-button" onClick={()=>navigateToView('app')}>← Back to app</button>
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
                onClick={()=> navigateToView('accounts')}
              >
                View Stored Accounts
              </button>
            </form>
            
             <div className="auth-card-footer">
               <p>Already have an account? <button className="link-button" onClick={()=> { 
                 setCurrentUser(null);
                try { sessionStorage.removeItem('currentUser'); localStorage.removeItem('currentUser'); localStorage.removeItem('rememberUser') } catch {}
                 navigateToView('login');
               }}>Sign in</button></p>
               <button className="back-link-button" onClick={()=>navigateToView('app')}>← Back to app</button>
             </div>
          </div>
        </div>
      </div>
    )
  }

  // Auth landing page (Log in / Sign Up)
  const posterFor = (path?: string | null, size: 'w185' | 'w342' | 'w500' | 'w780' = 'w185') => {
    return getImageUrl(path, size)
  }

  const backdropFor = (path?: string | null) => {
    return getImageUrl(path, 'w780')
  }

  if(view === 'auth'){
    return (
      <div className="auth-page-wrapper">
        <div className="auth-background-decoration">
          <div className="auth-gradient-overlay"></div>
        </div>
        <div className="auth-landing-content">
          <button className="back-link-button-top" onClick={()=>navigateToView('app')}>← Back to App</button>
          
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
            <button className="btn-primary btn-landing" onClick={()=>navigateToView('login')}>
              <span>Sign In</span>
              <span className="btn-arrow">→</span>
            </button>
            <button className="btn-secondary btn-landing" onClick={()=>navigateToView('signup')}>
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
          <button className="btn-link" onClick={()=>navigateToView('login')}>Back</button>
        </div>
        <p style={{fontSize:'12px', color:'#666', marginTop:'8px'}}>Passwords are stored hashed for security and shown here as stored values.</p>
      </div>
    )
  }

  if(view === 'add'){
    const handlePosterFile = (file: File | null) => {
      if(!file) {
        setAddPosterFile(null)
        if(addPosterPreview){
          URL.revokeObjectURL(addPosterPreview)
        }
        setAddPosterPreview(null)
        return
      }
      if(addPosterPreview){
        URL.revokeObjectURL(addPosterPreview)
      }
      const url = URL.createObjectURL(file)
      setAddPosterPreview(url)
      setAddPosterFile(file)
    }

    const handlePosterInputChange = (ev: React.ChangeEvent<HTMLInputElement>) => {
      const file = ev.target.files?.[0] ?? null
      handlePosterFile(file)
    }

    const handlePosterDrop = (ev: React.DragEvent<HTMLDivElement>) => {
      ev.preventDefault()
      ev.stopPropagation()
      const file = ev.dataTransfer.files?.[0] ?? null
      handlePosterFile(file)
    }

    const handlePosterDragOver = (ev: React.DragEvent<HTMLDivElement>) => {
      ev.preventDefault()
    }

    const onSubmitAdd = async (ev: React.FormEvent) => {
      ev.preventDefault()
      setAddError(null)
      if(!addTitle.trim()){
        setAddError('Title is required.')
        return
      }
      if(!addGenre.trim()){
        setAddError('Please provide a genre for this title.')
        return
      }
      setAddSubmitting(true)
      try{
        // Upload image file if one was selected
        let posterPath = addPosterPath.trim() || undefined
        if(addPosterFile){
          const uploadRes = await uploadImage(addPosterFile)
          if(!uploadRes.ok){
            setAddError(uploadRes.error || 'Failed to upload image. Please try again.')
            setAddSubmitting(false)
            return
          }
          posterPath = uploadRes.path
        }

        const payload: any = {
          media_type: addMediaType,
          title: addTitle.trim(),
          overview: addOverview.trim() || undefined,
          language: addLanguage.trim() || undefined,
          tmdb_score: addTmdbScore.trim() ? Number(addTmdbScore) : undefined,
          popularity: addPopularity.trim() ? Number(addPopularity) : undefined,
          poster_path: posterPath,
          genre: addGenre.trim(),
        }
        if(addYear.trim()){
          const yearNum = Number(addYear)
          if(Number.isNaN(yearNum)){
            setAddError('Year must be a number (e.g., 2024).')
            setAddSubmitting(false)
            return
          }
          if(addMediaType === 'movie'){
            payload.release_year = yearNum
          } else {
            payload.first_air_year = yearNum
          }
        }
        const res = await createMedia(payload)
        if(!res.ok){
          setAddError(res.error || 'Could not save title. Please try again.')
          return
        }
        // Reset form and take user back to Movies or TV tab, then reload list.
        const targetTab = addMediaType === 'movie' ? 'movies' : 'tv'
        setAddTitle('')
        setAddOverview('')
        setAddLanguage('')
        setAddYear('')
        setAddTmdbScore('')
        setAddPopularity('')
        setAddPosterPath('')
        if(addPosterPreview){
          URL.revokeObjectURL(addPosterPreview)
        }
        setAddPosterPreview(null)
        setAddPosterFile(null)
        setAddGenre('')
        setAddMediaType('movie')
        navigateToTab(targetTab)
      } catch (err:any){
        setAddError(err?.message || 'Unexpected error while saving title.')
      } finally {
        setAddSubmitting(false)
      }
    }

    return (
      <div className="container add-media-container">
        <header className="header">
          <div className="header-left">
            <div className="brand-group">
              <button type="button" className="brand-link" onClick={()=>{ navigateToTab('home'); }}>
                Movie &amp; TV Analytics
              </button>
            </div>
          </div>
          <div className="header-right">
            <div className="header-actions">
              {renderAccountControls()}
            </div>
          </div>
        </header>

        {/* Admin Hero Section */}
        <div className="add-media-hero">
          <div className="add-media-hero-content">
            <div className="add-media-hero-text">
              <h2>✨ Add New {addMediaType === 'movie' ? 'Movie' : 'TV Show'}</h2>
              <p>Expand your catalog with a new title. Fill in the details below to add it to your collection.</p>
            </div>
            <span className="add-media-hero-icon">{addMediaType === 'movie' ? '🎬' : '📺'}</span>
          </div>
        </div>

        <section className="add-media-layout">
          <div className="add-media-poster">
            <div
              className="poster-dropzone"
              onClick={() => addPosterInputRef.current?.click()}
              onDrop={handlePosterDrop}
              onDragOver={handlePosterDragOver}
            >
              {addPosterPreview ? (
                <img src={addPosterPreview} alt="Selected poster" className="poster-preview-image" />
              ) : (
                <div className="poster-dropzone-content">
                  <span className="poster-dropzone-icon">🖼️</span>
                  <span className="poster-dropzone-text">Drag & drop a poster image here<br/>or click to browse</span>
                  <span className="poster-dropzone-hint">Supports JPG, PNG, WebP</span>
                </div>
              )}
              <input
                ref={addPosterInputRef}
                type="file"
                accept="image/*"
                style={{ display: 'none' }}
                onChange={handlePosterInputChange}
              />
            </div>
            <div className="poster-score-card">
              <div className="poster-score-label">⭐ TMDb Score</div>
              <input
                className="form-input"
                type="number"
                step="0.1"
                min="0"
                max="10"
                value={addTmdbScore}
                onChange={e=>setAddTmdbScore(e.target.value)}
                placeholder="e.g. 7.8"
              />
            </div>
          </div>

          <form className="add-media-form" onSubmit={onSubmitAdd}>
            <div className="add-media-type-toggle">
              <button
                type="button"
                className={addMediaType === 'movie' ? 'active' : ''}
                onClick={()=> setAddMediaType('movie')}
              >
                🎬 Movie
              </button>
              <button
                type="button"
                className={addMediaType === 'tv' ? 'active' : ''}
                onClick={()=> setAddMediaType('tv')}
              >
                📺 TV Show
              </button>
            </div>

            {addError && (
              <div className="auth-error-message" style={{marginBottom: 16}}>
                <span className="error-icon">⚠️</span>
                <span>{addError}</span>
              </div>
            )}

            {/* Basic Info Section */}
            <div className="add-media-section">
              <div className="add-media-section-title">
                <span className="add-media-section-icon">📝</span>
                Basic Information
              </div>
              <div className="form-group">
                <label className="form-label">Title *</label>
                <input
                  className="form-input"
                  value={addTitle}
                  onChange={e=>setAddTitle(e.target.value)}
                  placeholder="Enter the title"
                  required
                />
              </div>

              <div className="add-media-inline-row">
                <div className="form-group">
                  <label className="form-label">📅 Year</label>
                  <input
                    className="form-input"
                    value={addYear}
                    onChange={e=>setAddYear(e.target.value)}
                    placeholder="e.g. 2024"
                  />
                </div>
                <div className="form-group">
                  <label className="form-label">🌐 Language</label>
                  <input
                    className="form-input"
                    value={addLanguage}
                    onChange={e=>setAddLanguage(e.target.value)}
                    placeholder="e.g. en"
                  />
                </div>
                <div className="form-group">
                  <label className="form-label">📈 Popularity</label>
                  <input
                    className="form-input"
                    type="number"
                    step="0.1"
                    value={addPopularity}
                    onChange={e=>setAddPopularity(e.target.value)}
                    placeholder="e.g. 25.3"
                  />
                </div>
              </div>
            </div>

            {/* Classification Section */}
            <div className="add-media-section">
              <div className="add-media-section-title">
                <span className="add-media-section-icon">🏷️</span>
                Classification
              </div>
              <div className="form-group">
                <label className="form-label">Genre *</label>
                <div className="genre-input-wrapper">
                  <span className="genre-input-icon">🎭</span>
                  <input
                    className="form-input"
                    value={addGenre}
                    onChange={e=>setAddGenre(e.target.value)}
                    placeholder="e.g. Drama, Action, Comedy"
                    required
                  />
                </div>
                <span className="genre-hint">Separate multiple genres with commas</span>
              </div>
            </div>

            {/* Media Section */}
            <div className="add-media-section">
              <div className="add-media-section-title">
                <span className="add-media-section-icon">🖼️</span>
                Media & Content
              </div>
              <div className="form-group">
                <label className="form-label">Poster Path / URL</label>
                <input
                  className="form-input"
                  value={addPosterPath}
                  onChange={e=>setAddPosterPath(e.target.value)}
                  placeholder="/path/on/tmdb.jpg or https://…"
                />
              </div>

              <div className="form-group">
                <label className="form-label">Synopsis</label>
                <textarea
                  className="form-input"
                  rows={4}
                  value={addOverview}
                  onChange={e=>setAddOverview(e.target.value)}
                  placeholder="Write a brief description of the movie or TV show..."
                />
              </div>
            </div>

            <div className="add-media-actions">
              <button
                type="button"
                className="add-media-cancel"
                onClick={()=> navigateToTab('movies')}
              >
                Cancel
              </button>
              <button
                className="add-media-submit"
                type="submit"
                disabled={addSubmitting}
              >
                {addSubmitting ? '⏳ Saving…' : '✓ Add to Catalog'}
              </button>
            </div>
          </form>
        </section>
      </div>
    )
  }

  if(view === 'detail'){
    let chipLabel = 'TITLE'
    if(detailData?.media_type){
      chipLabel = detailData.media_type.toUpperCase()
    } else if(detailData && 'movie_id' in detailData){
      chipLabel = 'MOVIE'
    } else if(detailData && 'show_id' in detailData){
      chipLabel = 'TV'
  }
    const rawLanguage = typeof detailData?.original_language === 'string' ? detailData.original_language.trim() : ''
    const hasLanguage = rawLanguage.length > 0
    const languageCode = hasLanguage ? rawLanguage.toUpperCase() : null
    const friendlyLanguage = hasLanguage ? formatLanguageLabel(rawLanguage) : null
    const languageTitle = friendlyLanguage
      ? languageCode && friendlyLanguage.toUpperCase() !== languageCode
        ? `${friendlyLanguage} (${languageCode})`
        : friendlyLanguage
      : hasLanguage
        ? languageCode
        : 'Language unavailable'

  return (
      <div className="detail-container">
        {/* Header */}
        <div className="detail-header-shell">
          <header className={`header detail-page-header ${mobileSearchOpen ? 'header-mobile-search' : ''}`}>
            <div className="header-left">
            <div className="brand-group">
              <button type="button" className="brand-link" onClick={()=>{ navigateToTab('home'); }}>
                Movie &amp; TV Analytics
              </button>
            </div>
            {renderSearchField()}
              <button
                type="button"
                className="search-toggle"
                aria-label="Toggle search"
                aria-expanded={mobileSearchOpen}
                onClick={()=>setMobileSearchOpen(prev => !prev)}
              >
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
                  <circle cx="11" cy="11" r="6" />
                  <line x1="15.5" y1="15.5" x2="21" y2="21" />
                </svg>
              </button>
            </div>
            <div className="header-right">
              <nav className="header-nav" aria-label="Primary navigation">
                {primaryNav.map(item => (
                  <button
                    key={item.id}
                    type="button"
                    onClick={()=>{ navigateToTab(item.id); }}
                    aria-current={tab === item.id ? 'page' : undefined}
                    className={`nav-pill ${tab === item.id ? 'active' : ''}`}
                  >
                    {item.label}
                  </button>
                ))}
              </nav>
              <div className="header-actions">
                {renderAccountControls()}
              </div>
            </div>
          </header>
        </div>

        {detailLoading && (
          <div className="detail-loading">
            <div className="loading-spinner"></div>
            <p>Loading details...</p>
          </div>
        )}

        {detailError && (
          <div className="detail-error">
            <span className="error-icon">⚠️</span>
            <p>{detailError}</p>
          </div>
        )}

        {detailData && (
          <div className="detail-view">
            {/* Backdrop Hero Section */}
            <div className="detail-backdrop" style={{
              backgroundImage: detailData.backdrop_path 
                ? `url(${getImageUrl(detailData.backdrop_path, 'w780')})`
                : 'linear-gradient(135deg, #1a1a1f 0%, #0d0d10 100%)'
            }}>
              <div className="detail-backdrop-overlay"></div>
            </div>

            {/* Main Content */}
            <div className="detail-content">
              <div className="detail-main">
                {/* Poster Card */}
                <div className="detail-poster-card">
                  <div className="detail-poster-wrapper">
                    {detailData.poster_path ? (
                      <img 
                        src={getImageUrl(detailData.poster_path, 'w500')} 
                        alt={detailData.title}
                        className="detail-poster-img"
                      />
                    ) : (
                      <div className="detail-poster-empty">
                        <span className="poster-icon">🎬</span>
                        <span>No Poster</span>
                      </div>
                    )}
                  </div>
                  
                  {/* Quick Stats on Poster */}
                  <div className="detail-poster-stats">
                    <div className="poster-stat">
                      <div className="poster-stat-icon">⭐</div>
                      <div className="poster-stat-content">
                        <div className="poster-stat-value">{detailData.vote_average?.toFixed(1) ?? 'N/A'}</div>
                        <div className="poster-stat-label">TMDb Score</div>
                      </div>
                    </div>
                    {detailData.vote_count && (
                      <div className="poster-stat-sub">{detailData.vote_count.toLocaleString()} votes</div>
                    )}
                  </div>
                </div>

                {/* Info Panel */}
                <div className="detail-info-panel">
                  <div className="detail-heading">
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', width: '100%' }}>
                      <div>
                        <div className="detail-type-badge">{chipLabel}</div>
                        {detailEditMode ? (
                          <input
                            type="text"
                            value={editTitle}
                            onChange={(e) => setEditTitle(e.target.value)}
                            className="detail-edit-input detail-edit-title"
                            placeholder="Title"
                          />
                        ) : (
                          <h1 className="detail-main-title">{detailData.title}</h1>
                        )}
                      </div>
                      <button
                        type="button"
                        className={`detail-edit-button ${detailEditMode ? 'detail-edit-button-active' : ''}`}
                        onClick={async () => {
                          if(detailEditMode) {
                            // Save changes
                            if(!detailData) return
                            setEditSaving(true)
                            try {
                              const payload: UpdateMediaPayload = {
                                title: editTitle || undefined,
                                overview: editOverview || undefined,
                                language: editLanguage || undefined,
                                tmdb_score: editTmdbScore ? parseFloat(editTmdbScore) : undefined,
                                popularity: editPopularity ? parseFloat(editPopularity) : undefined,
                                genre: editGenre || undefined,
                              }
                              
                              if(detailData.media_type === 'movie') {
                                const movieDetail = detailData as MovieDetail
                                if(editYear) {
                                  payload.release_year = parseInt(editYear)
                                }
                                const result = await updateMedia('movie', movieDetail.movie_id, payload)
                                if(!result.ok) {
                                  alert(`Failed to update: ${result.error}`)
                                  return
                                }
                              } else {
                                const showDetail = detailData as ShowDetail
                                if(editYear) {
                                  payload.first_air_year = parseInt(editYear)
                                }
                                const result = await updateMedia('tv', showDetail.show_id, payload)
                                if(!result.ok) {
                                  alert(`Failed to update: ${result.error}`)
                                  return
                                }
                              }
                              
                              // Reload detail data
                              const mediaType = detailData.media_type
                              const id = mediaType === 'movie' ? (detailData as MovieDetail).movie_id : (detailData as ShowDetail).show_id
                              if(mediaType === 'movie') {
                                const data = await getMovieDetail(id)
                                setDetailData({ ...data, media_type: 'movie' })
                                setEditTitle(data.title || '')
                                setEditOverview(data.overview || '')
                                setEditLanguage(data.original_language || '')
                                setEditYear(data.release_year?.toString() || '')
                                setEditTmdbScore(data.vote_average?.toString() || '')
                                setEditPopularity(data.popularity?.toString() || '')
                                // Join all genres with comma and space
                                setEditGenre(data.genres?.join(', ') || '')
                              } else {
                                const data = await getShowDetail(id)
                                setDetailData({ ...data, media_type: 'tv' })
                                setEditTitle(data.title || '')
                                setEditOverview(data.overview || '')
                                setEditLanguage(data.original_language || '')
                                const yearMatch = data.first_air_date?.match(/^(\d{4})/)
                                setEditYear(yearMatch ? yearMatch[1] : '')
                                setEditTmdbScore(data.vote_average?.toString() || '')
                                setEditPopularity(data.popularity?.toString() || '')
                                // Join all genres with comma and space
                                setEditGenre(data.genres?.join(', ') || '')
                              }
                              setDetailEditMode(false)
                            } catch (err: any) {
                              alert(`Error updating: ${err?.message || 'Unknown error'}`)
                            } finally {
                              setEditSaving(false)
                            }
                          } else {
                            // Enter edit mode - initialize fields from current data
                            setEditTitle(detailData.title || '')
                            setEditOverview(detailData.overview || '')
                            setEditLanguage(detailData.original_language || '')
                            if(detailData.media_type === 'movie') {
                              const movieDetail = detailData as MovieDetail
                              setEditYear(movieDetail.release_year?.toString() || '')
                            } else {
                              const showDetail = detailData as ShowDetail
                              const yearMatch = showDetail.first_air_date?.match(/^(\d{4})/)
                              setEditYear(yearMatch ? yearMatch[1] : '')
                            }
                            setEditTmdbScore(detailData.vote_average?.toString() || '')
                            setEditPopularity(detailData.popularity?.toString() || '')
                            // Join all genres with comma and space
                            setEditGenre(detailData.genres?.join(', ') || '')
                            setDetailEditMode(true)
                          }
                        }}
                        disabled={editSaving || detailLoading}
                      >
                        {detailEditMode ? (editSaving ? '⏳ Saving...' : '✓ Save Changes') : '✏️ Edit'}
                      </button>
                    </div>
                  </div>

                  {/* Meta Info Row */}
                  {detailEditMode ? (
                    <div className="detail-edit-form">
                      <div className="detail-edit-header">
                        <span className="detail-edit-header-icon">✏️</span>
                        <span className="detail-edit-header-text">Edit {detailData.media_type === 'movie' ? 'Movie' : 'TV Show'} Details</span>
                      </div>
                      <div className="detail-edit-grid">
                        <div className="detail-edit-row">
                          <label>📅 Year</label>
                          <input
                            type="number"
                            value={editYear}
                            onChange={(e) => setEditYear(e.target.value)}
                            className="detail-edit-input"
                            placeholder="e.g. 2024"
                          />
                        </div>
                        <div className="detail-edit-row">
                          <label>🌐 Language</label>
                          <input
                            type="text"
                            value={editLanguage}
                            onChange={(e) => setEditLanguage(e.target.value)}
                            className="detail-edit-input"
                            placeholder="e.g. en"
                          />
                        </div>
                        <div className="detail-edit-row">
                          <label>⭐ TMDb Score</label>
                          <input
                            type="number"
                            step="0.1"
                            value={editTmdbScore}
                            onChange={(e) => setEditTmdbScore(e.target.value)}
                            className="detail-edit-input"
                            placeholder="0-10"
                          />
                        </div>
                        <div className="detail-edit-row">
                          <label>📈 Popularity</label>
                          <input
                            type="number"
                            step="0.1"
                            value={editPopularity}
                            onChange={(e) => setEditPopularity(e.target.value)}
                            className="detail-edit-input"
                            placeholder="e.g. 25.3"
                          />
                        </div>
                        <div className="detail-edit-row detail-edit-row-full">
                          <label>🎭 Genre</label>
                          <div className="detail-edit-genre-input">
                            <span className="detail-edit-genre-icon">🏷️</span>
                            <input
                              type="text"
                              value={editGenre}
                              onChange={(e) => setEditGenre(e.target.value)}
                              className="detail-edit-input"
                              placeholder="e.g. Action, Drama, Comedy"
                            />
                          </div>
                          <span className="detail-edit-hint">Separate multiple genres with commas</span>
                        </div>
                        <div className="detail-edit-row detail-edit-row-full">
                          <label>📝 Synopsis</label>
                          <textarea
                            value={editOverview}
                            onChange={(e) => setEditOverview(e.target.value)}
                            className="detail-edit-textarea"
                            placeholder="Write a brief description..."
                            rows={5}
                          />
                        </div>
                      </div>
                    </div>
                  ) : (
                    <>
                      <div className="detail-meta-row">
                        {detailData.media_type === 'movie' && (detailData as MovieDetail).release_year && (
                          <div className="meta-item">
                            <span className="meta-icon">📅</span>
                            <span>{(detailData as MovieDetail).release_year}</span>
                          </div>
                        )}
                        {detailData.media_type === 'tv' && (detailData as ShowDetail).first_air_date && (
                          <div className="meta-item">
                            <span className="meta-icon">📅</span>
                            <span>{(detailData as ShowDetail).first_air_date?.substring(0, 4) ?? 'Unknown'}</span>
                          </div>
                        )}
                        {detailData.media_type === 'movie' && (detailData as MovieDetail).runtime_minutes != null && (detailData as MovieDetail).runtime_minutes! > 0 && (
                          <div className="meta-item">
                            <span className="meta-icon">⏱️</span>
                            <span>{(detailData as MovieDetail).runtime_minutes} min</span>
                          </div>
                        )}
                        {detailData.media_type === 'tv' && (detailData as ShowDetail).season_count !== undefined && (
                          <div className="meta-item">
                            <span className="meta-icon">📺</span>
                            <span>{(detailData as ShowDetail).season_count} season{(detailData as ShowDetail).season_count !== 1 ? 's' : ''}</span>
                          </div>
                        )}
                      </div>

                      {/* Genres */}
                      <div className="detail-genre-list">
                        <span className="detail-genre-tag detail-language-tag" title={`Original language: ${languageTitle}`}>
                          <span className="language-icon" aria-hidden="true">🌐</span>
                          <span>{languageTitle}</span>
                        </span>
                        {detailData.genres.map(g => (
                          <span key={g} className="detail-genre-tag">{g}</span>
                        ))}
                      </div>

                      {/* Overview */}
                      <div className="detail-overview-section">
                        <h3 className="section-title">Synopsis</h3>
                        {detailData.overview ? (
                          <p className="detail-synopsis">{detailData.overview}</p>
                        ) : (
                          <p className="detail-synopsis no-data">No synopsis available.</p>
                        )}
                      </div>
                    </>
                  )}

                  {/* Stats Grid */}
                  <div className="detail-stats-section">
                    <h3 className="section-title">Statistics</h3>
                    <div className="detail-stats-grid">
                      {detailData.user_avg_rating && (
                        <div className="stat-card">
                          <div className="stat-icon">👥</div>
                          <div className="stat-value">{detailData.user_avg_rating.toFixed(1)}</div>
                          <div className="stat-label">User Rating</div>
                        </div>
                      )}
                      <div className="stat-card">
                        <div className="stat-icon">💬</div>
                        <div className="stat-value">{reviews.length}</div>
                        <div className="stat-label">Reviews</div>
                      </div>
                      {detailData.popularity && (
                        <div className="stat-card">
                          <div className="stat-icon">🔥</div>
                          <div className="stat-value">{detailData.popularity.toFixed(0)}</div>
                          <div className="stat-label">Popularity</div>
                        </div>
                      )}
                      {detailData.media_type === 'movie' && (detailData as MovieDetail).budget && (detailData as MovieDetail).budget! > 0 && (
                        <div className="stat-card">
                          <div className="stat-icon">💰</div>
                          <div className="stat-value">${((detailData as MovieDetail).budget! / 1000000).toFixed(1)}M</div>
                          <div className="stat-label">Budget</div>
                        </div>
                      )}
                      {detailData.media_type === 'movie' && (detailData as MovieDetail).revenue && (detailData as MovieDetail).revenue! > 0 && (
                        <div className="stat-card">
                          <div className="stat-icon">📈</div>
                          <div className="stat-value">${((detailData as MovieDetail).revenue! / 1000000).toFixed(1)}M</div>
                          <div className="stat-label">Box Office</div>
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Reviews Section */}
                  <div className="detail-reviews-section">
                    <h3 className="section-title">Reviews</h3>
                    <div className="review-count-display">
                      There are {reviews.length} review{reviews.length !== 1 ? 's' : ''}
                    </div>
                    
                    {/* Display Reviews */}
                    {reviewsLoading ? (
                      <div className="reviews-loading">Loading reviews...</div>
                    ) : reviews.length > 0 ? (
                      <div className="reviews-list">
                        {reviews.map((review) => (
                          <div key={review.review_id} className="review-item">
                            <div className="review-content">{review.content}</div>
                            <div className="review-meta">
                              {review.user_email && (
                                <span className="review-author">{review.user_email.split('@')[0]}</span>
                              )}
                              {review.created_at && (
                                <span className="review-date">
                                  {new Date(review.created_at).toLocaleDateString()}
                                </span>
                              )}
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="reviews-empty">No reviews yet. Be the first to review!</div>
                    )}
                    
                    {/* Review Input */}
                    {currentUser && (
                      <div className="review-input-section">
                        <textarea
                          value={reviewText}
                          onChange={(e) => setReviewText(e.target.value)}
                          className="review-input-textarea"
                          placeholder="Enter Your Review Here"
                          rows={3}
                        />
                        <button
                          type="button"
                          className="review-submit-button"
                          onClick={async () => {
                            if(!reviewText.trim()) {
                              alert('Please enter a review before submitting.')
                              return
                            }
                            if(!currentUser) {
                              alert('Please log in to submit a review.')
                              return
                            }
                            if(!detailData) return
                            
                            setReviewSubmitting(true)
                            try {
                              // Get user_id if not already available
                              let userId = currentUser.user_id
                              if(!userId && currentUser.email) {
                                // Fetch user_id from backend by email
                                try {
                                  const userData = await getUserByEmail(currentUser.email)
                                  console.log('[Review] getUserByEmail result:', userData)
                                  if(userData.ok && userData.user_id) {
                                    userId = userData.user_id
                                    // Update currentUser with user_id
                                    const updatedUser = { ...currentUser, user_id: userId }
                                    setCurrentUser(updatedUser)
                                    try {
                                      sessionStorage.setItem('currentUser', JSON.stringify(updatedUser))
                                      if(localStorage.getItem('rememberUser') === '1') {
                                        localStorage.setItem('currentUser', JSON.stringify(updatedUser))
                                      }
                                    } catch {}
                                  } else {
                                    console.error('[Review] Failed to get user_id:', userData.error)
                                    alert(`Unable to identify user: ${userData.error || 'Unknown error'}. Please log out and log back in.`)
                                    setReviewSubmitting(false)
                                    return
                                  }
                                } catch (err: any) {
                                  console.error('[Review] Error fetching user_id:', err)
                                  alert(`Error identifying user: ${err?.message || 'Unknown error'}. Please log out and log back in.`)
                                  setReviewSubmitting(false)
                                  return
                                }
                              }
                              
                              if(!userId) {
                                alert('Unable to identify user. Please log out and log back in to refresh your session.')
                                setReviewSubmitting(false)
                                return
                              }
                              
                              const mediaType = detailData.media_type
                              const targetType = mediaType === 'movie' ? 'movie' : 'show' // Backend expects 'show' not 'tv'
                              const id = mediaType === 'movie' 
                                ? (detailData as MovieDetail).movie_id 
                                : (detailData as ShowDetail).show_id
                              
                              const result = await createReview(
                                userId,
                                targetType,
                                id,
                                reviewText.trim()
                              )
                              
                              if(result.ok) {
                                setReviewText('')
                                // Reload reviews
                                const reviewsData = await getReviews(targetType, id)
                                setReviews(reviewsData.reviews || [])
                              } else {
                                alert(`Failed to submit review: ${result.error}`)
                              }
                            } catch (err: any) {
                              alert(`Error submitting review: ${err?.message || 'Unknown error'}`)
                            } finally {
                              setReviewSubmitting(false)
                            }
                          }}
                          disabled={reviewSubmitting || !reviewText.trim() || !currentUser}
                        >
                          {reviewSubmitting ? 'Submitting...' : 'Submit'}
                        </button>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    )
  }

  return (
    <div className="container">
      <header className={`header ${mobileSearchOpen ? 'header-mobile-search' : ''}`}>
        <div className="header-left">
          <div className="brand-group">
            <button type="button" className="brand-link" onClick={()=>{ navigateToTab('home'); }}>
              Movie &amp; TV Analytics
            </button>
          </div>
          {renderSearchField()}
          <button
            type="button"
            className="search-toggle"
            aria-label="Toggle search"
            aria-expanded={mobileSearchOpen}
            onClick={()=>setMobileSearchOpen(prev => !prev)}
          >
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
              <circle cx="11" cy="11" r="6" />
              <line x1="15.5" y1="15.5" x2="21" y2="21" />
            </svg>
          </button>
        </div>
        <div className="header-right">
          <nav className="header-nav" aria-label="Primary navigation">
            {primaryNav.map(item => (
              <button
                key={item.id}
                type="button"
                onClick={()=>navigateToTab(item.id)}
                aria-current={tab === item.id ? 'page' : undefined}
                className={`nav-pill ${tab === item.id ? 'active' : ''}`}
              >
                {item.label}
              </button>
            ))}
          </nav>
          <div className="header-actions">
            {renderAccountControls()}
          </div>
        </div>
      </header>
      {tab==='home' && (
        <section className="hero">
          <h1>Movie &amp; TV Analytics</h1>
          <p>Track trends, analyze ratings, and discover insights across your favorite movies and shows.</p>
          <div className="hero-actions">
            <button className="btn-solid btn-lg" onClick={()=>navigateToTab('analytics')}>Get Started</button>
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
              {carouselLoading ? (
                <div className="trending-hero-empty">Loading trending titles…</div>
              ) : heroSlides.length === 0 ? (
                <div className="trending-hero-empty">
                  Run the TMDb ETL loader to populate trending titles.
                </div>
              ) : (
                heroSlides.map((item, idx) => {
                  const isActive = idx === activeHeroIndex
                  const backdrop = backdropFor(item.backdrop_url ?? item.poster_url ?? null)
                  const overview =
                    item.overview && item.overview.length > 220
                      ? `${item.overview.slice(0, 217)}…`
                      : item.overview
                  const rating = item.tmdb_vote_avg
                  const targetMediaType = item.media_type === 'movie' ? 'movie' : 'tv'
                  const handleClick = () => {
                    navigateToDetail(targetMediaType, item.item_id || undefined)
                  }
                  return (
                    <button
                      key={`hero-${item.media_type}-${item.tmdb_id}-${idx}`}
                      type="button"
                      className={`trending-hero-slide${isActive ? ' active' : ''}`}
                      onClick={handleClick}
                      aria-label={`View ${item.title}`}
                      tabIndex={isActive ? 0 : -1}
                      aria-hidden={!isActive}
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
                          <span>⭐ {rating != null ? rating.toFixed(1) : '—'}</span>
                          {item.release_date && <span>📅 {item.release_date}</span>}
                          {item.genres.length > 0 && <span>{item.genres.slice(0, 2).join(' • ')}</span>}
                        </div>
                      </div>
                    </button>
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
                    <span className="trending-hero-nav-icon">‹</span>
                  </button>
                  <button
                    type="button"
                    className="trending-hero-nav next"
                    onClick={() => setCarouselIndex(prev => (prev + 1) % heroSlides.length)}
                    aria-label="Next slide"
                  >
                    <span className="trending-hero-nav-icon">›</span>
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
                  <h3>Popular</h3>
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

              <div 
                className={`trending-rail-list trending-rail-list-${trendingFadeState}`}
              >
                {trending.length === 0 && trendingLoading && (
                  <>
                    {[...Array(10)].map((_, i) => (
                      <div key={`skeleton-${i}`} className="trending-rail-item trending-rail-skeleton">
                        <div className="rank">{i + 1}</div>
                        <div className="thumb skeleton-box"></div>
                        <div className="details">
                          <div className="skeleton-line skeleton-line-title"></div>
                          <div className="skeleton-line skeleton-line-meta"></div>
                          <div className="skeleton-line skeleton-line-genre"></div>
                        </div>
                      </div>
                    ))}
                  </>
                )}
                {trending.length === 0 && !trendingLoading && (
                  <div className="trending-rail-empty">
                    {trendingError ?? 'Run the TMDb ETL loader to populate trending titles.'}
                  </div>
                )}
                {trending.length > 0 && trending.map((item, index) => {
                  const poster = posterFor(item.poster_url, 'w185')
                  const genres = item.genres.length > 0 ? item.genres.slice(0, 2).join(' • ') : '—'
                  const targetMediaType = item.media_type === 'movie' ? 'movie' : 'tv'
                  const handleClick = () => {
                    navigateToDetail(targetMediaType, item.item_id || undefined)
                  }
                  return (
                    <button
                      key={`leaderboard-${item.media_type}-${item.tmdb_id}-${index}`}
                      type="button"
                      className="trending-rail-item"
                      onClick={handleClick}
                      aria-label={`View details for ${item.title}`}
                    >
                      <div className="rank">{index + 1}</div>
                      <div className="thumb">
                        {poster ? <img src={poster} alt={item.title} loading="lazy" /> : <div className="thumb-placeholder">🎬</div>}
                      </div>
                      <div className="details">
                        <div className="title" title={item.title}>{item.title}</div>
                        <div className="meta">
                          <span>{item.media_type === 'movie' ? 'Movie' : 'TV'}</span>
                          <span>⭐ {item.tmdb_vote_avg != null ? item.tmdb_vote_avg.toFixed(1) : '—'}</span>
                        </div>
                        <div className="genres">{genres}</div>
                      </div>
                    </button>
                  )
                })}
              </div>
            </aside>
          </div>

          <div className="new-release-section">
            <div className="new-release-header">
              <div>
                <h3>New Releases</h3>
                <p>Fresh arrivals from {new Date().getFullYear()}.</p>
              </div>
              <div className="new-release-actions">
                <div className="new-release-tabs">
                  {(['all', 'movie', 'tv'] as ReleaseFilter[]).map(option => (
                    <button
                      key={option}
                      type="button"
                      className={option === newReleaseFilter ? 'active' : ''}
                      onClick={() => { setNewReleaseFilter(option); setNewReleasePage(0) }}
                      disabled={newReleasesLoading && option === newReleaseFilter}
                    >
                      {option === 'all' ? 'All' : option === 'movie' ? 'Movies' : 'TV Shows'}
                    </button>
                  ))}
                </div>
                <button
                  type="button"
                  className="btn-text"
                  onClick={() => loadNewReleases(NEW_RELEASE_FETCH_LIMIT, newReleaseFilter)}
                  disabled={newReleasesLoading}
                >
                  {newReleasesLoading ? 'Refreshing…' : 'Refresh list'}
                </button>
              </div>
            </div>

            {newReleasesLoading && newReleases.length === 0 && (
              <div className="new-release-empty">Loading new releases…</div>
            )}
            {!newReleasesLoading && newReleasesError && (
              <div className="new-release-empty">{newReleasesError}</div>
            )}
            {!newReleasesLoading &&
              !newReleasesError &&
              newReleases.length === 0 && (
                <div className="new-release-empty">
                  No new releases found. Try running the ETL loader.
                </div>
              )}
            {newReleases.length > 0 && (
              <>
                <div className={`new-release-grid-wrapper ${newReleaseTransitionType === 'fade' ? `new-release-grid-wrapper-${newReleasesFadeState}` : ''}`}>
                  <div className={`new-release-grid ${newReleaseTransitionType === 'slide' ? `new-release-grid-slide-${newReleaseSlideDirection}` : ''}`}>
                    {displayedNewReleases.map(item => (
                      <Card 
                        key={`nr-${item.media_type}-${item.tmdb_id}`} 
                        item={item} 
                        onClick={() => navigateToDetail(item.media_type, item.id!)}
                      />
                    ))}
                  </div>
                </div>
                {newReleaseTotalPages > 1 && (
                  <div className="pagination-controls" aria-label="New releases pagination">
                    <button
                      type="button"
                      onClick={() => {
                        setNewReleaseTransitionType('slide')
                        setNewReleaseSlideDirection('right')
                        setNewReleasePage(prev => Math.max(0, prev - 1))
                      }}
                      disabled={newReleasesLoading || newReleasePage === 0}
                    >
                      ‹ Previous
                    </button>
                    <button
                      type="button"
                      onClick={() => {
                        setNewReleaseTransitionType('slide')
                        setNewReleaseSlideDirection('left')
                        setNewReleasePage(prev => Math.min(newReleaseTotalPages - 1, prev + 1))
                      }}
                      disabled={newReleasesLoading || newReleasePage >= newReleaseTotalPages - 1}
                    >
                      Next ›
                    </button>
                  </div>
                )}
              </>
            )}
          </div>

          {/* Footer removed per design update */}
        </section>
      )}


      <div className="toolbar">
        <button onClick={onRefresh} disabled={busy}>↻ Refresh (ingest 1 page)</button>
        <span className="hint">Set TMDB_API_KEY in backend .env to enable refresh/search.</span>
      </div>

      {tab==='analytics' && (
        <section className="analytics-section">
          <div className="analytics-hero">
            <div className="analytics-hero-copy">
              <span className="analytics-kicker">Library Snapshot</span>
              <h2>Insights &amp; Trends</h2>
              <p>
                {analyticsSnapshot.totalItems
                  ? `Tracking ${analyticsSnapshot.totalItems.toLocaleString()} titles across ${analyticsSnapshot.languageCount} languages.`
                  : 'Your catalog is ready for fresh data—run the TMDb loader to start populating insights.'}
              </p>
              <div className="analytics-tags">
                {analyticsSnapshot.highlightGenre && (
                  <span className="analytics-tag">Top genre · {analyticsSnapshot.highlightGenre}</span>
                )}
                {analyticsSnapshot.highlightLanguage && (
                  <span className="analytics-tag">
                    Most common language · {formatLanguageLabel(analyticsSnapshot.highlightLanguage) ?? analyticsSnapshot.highlightLanguage.toUpperCase()}
                  </span>
                )}
              </div>
            </div>
            <div className="analytics-hero-summary">
              <span className="analytics-total-value">
                {analyticsSnapshot.totalItems ? analyticsSnapshot.totalItems.toLocaleString() : '—'}
              </span>
              <span className="analytics-total-label">Titles Indexed</span>
              <div className="analytics-total-breakdown">
                <span>{analyticsSnapshot.totalMovies.toLocaleString()} movies</span>
                <span>{analyticsSnapshot.totalTvShows.toLocaleString()} TV shows</span>
              </div>
            </div>
          </div>

          <div className="analytics-metrics">
            <Stat
              label="Average Rating"
              value={analyticsSnapshot.avgRating ? analyticsSnapshot.avgRating.toFixed(1) : '—'}
              hint="Across all titles"
            />
            <Stat
              label="Languages"
              value={analyticsSnapshot.languageCount.toLocaleString()}
              hint="Unique originals"
            />
            <Stat
              label="Movies"
              value={analyticsSnapshot.totalMovies.toLocaleString()}
              hint={`${analyticsSnapshot.movieShare}% of library`}
            />
            <Stat
              label="TV Series"
              value={analyticsSnapshot.totalTvShows.toLocaleString()}
              hint={`${analyticsSnapshot.tvShare}% of library`}
            />
          </div>

          <div className="analytics-panels">
            <article className="analytics-card">
              <header className="analytics-card-header">
                <h3>Genre Leaderboard</h3>
                {analyticsSnapshot.highlightGenre && (
                  <span className="analytics-card-subtitle">
                    {analyticsSnapshot.highlightGenre} leads with {analyticsSnapshot.highlightGenreCount} titles
                  </span>
                )}
              </header>
              <div className="analytics-genre-list">
                {analyticsSnapshot.topGenres.length > 0 ? (
                  analyticsSnapshot.topGenres.map((g: any) => {
                    const ratio =
                      analyticsSnapshot.highestGenreCount > 0
                        ? Math.min(100, Math.max(6, (g.count / analyticsSnapshot.highestGenreCount) * 100))
                        : 0
                    return (
                      <div className="analytics-genre-row" key={g.genre || 'unknown'}>
                        <span className="analytics-genre-name">{g.genre || '—'}</span>
                        <div className="analytics-progress-bar" aria-hidden="true">
                          <div className="analytics-progress" style={{ width: `${ratio}%` }} />
                        </div>
                        <span className="analytics-genre-count">{g.count ?? 0}</span>
                      </div>
                    )
                  })
                ) : (
                  <div className="analytics-empty-card">
                    Genre insights will appear once titles have been ingested.
                  </div>
                )}
              </div>
            </article>

            <article className="analytics-card">
              <header className="analytics-card-header">
                <h3>Language Footprint</h3>
                <span className="analytics-card-subtitle">
                  {analyticsSnapshot.languageCount} languages represented
                </span>
              </header>
              <div className="analytics-language-grid">
                {analyticsSnapshot.languages.length > 0 ? (
                  analyticsSnapshot.languages.map((l: any) => {
                    const pretty =
                      formatLanguageLabel(l.language) ?? l.language?.toUpperCase?.() ?? '—'
                    return (
                      <div className="analytics-language-chip" key={`${l.language}-${l.count}`}>
                        <span className="analytics-language-name">{pretty}</span>
                        <span className="analytics-language-count">{l.count}</span>
                      </div>
                    )
                  })
                ) : (
                  <div className="analytics-empty-card">
                    Language data appears once the library has source material.
                  </div>
                )}
              </div>
            </article>

            <article className="analytics-card analytics-card-stretch">
              <header className="analytics-card-header">
                <h3>Format Split</h3>
                <span className="analytics-card-subtitle">Movies vs TV inventory</span>
              </header>
              <div
                className="analytics-ratio-bar"
                role="img"
                aria-label={`Library is ${analyticsSnapshot.movieShare}% movies and ${analyticsSnapshot.tvShare}% TV shows`}
              >
                <div className="analytics-ratio-segment movies" style={{ width: `${analyticsSnapshot.movieShare}%` }} />
                <div className="analytics-ratio-segment tv" style={{ width: `${analyticsSnapshot.tvShare}%` }} />
                {analyticsSnapshot.otherShare > 0 && (
                  <div className="analytics-ratio-segment other" style={{ width: `${analyticsSnapshot.otherShare}%` }} />
                )}
              </div>
              <div className="analytics-ratio-legend">
                <span>
                  <span className="analytics-dot movies" />
                  {analyticsSnapshot.totalMovies.toLocaleString()} movies
                </span>
                <span>
                  <span className="analytics-dot tv" />
                  {analyticsSnapshot.totalTvShows.toLocaleString()} series
                </span>
                {analyticsSnapshot.otherShare > 0 && (
                  <span>
                    <span className="analytics-dot other" />
                    {Math.max(0, analyticsSnapshot.totalItems - analyticsSnapshot.totalMovies - analyticsSnapshot.totalTvShows).toLocaleString()} other
                  </span>
                )}
              </div>
            </article>
          </div>
        </section>
      )}

      {tab==='movies' && (
        <section className="tab-section tab-section-movies">
          <div className="list-filters-wrapper">
            <div className="filter-select-group">
              <select
                className="filter-select"
                value={moviesPendingGenre}
                onChange={(e) => setMoviesPendingGenre(e.target.value)}
                disabled={moviesLoading && !moviesReady}
              >
                <option value="all">All Genres</option>
                {availableGenres.map(g => (
                  <option key={g} value={g}>{g}</option>
                ))}
              </select>
              <select
                className="filter-select"
                value={moviesPendingLanguage}
                onChange={(e) => setMoviesPendingLanguage(e.target.value)}
                disabled={moviesLoading && !moviesReady}
              >
                <option value="all">All Languages</option>
                {availableLanguages.map(lang => (
                  <option key={lang} value={lang}>{lang.toUpperCase()}</option>
                ))}
              </select>
              <select
                className="filter-select"
                value={moviesPendingSort}
                onChange={(e) => setMoviesPendingSort(e.target.value)}
                disabled={moviesLoading && !moviesReady}
              >
                <option value="popularity">Popularity</option>
                <option value="rating">Rating</option>
                <option value="title">Title</option>
                <option value="release_date">Release Date</option>
              </select>
              <button
                type="button"
                className={`filter-apply-button${moviesFiltersDirty ? ' active' : ''}`}
                onClick={applyMoviesFilters}
                disabled={moviesLoading}
              >
                Search
              </button>
            </div>
            <div className="filter-pagination">
              <button
                type="button"
                className={`filter-action-button${moviesSelectionMode ? ' active' : ''}`}
                onClick={() => {
                  setMoviesSelectionMode(!moviesSelectionMode)
                  if(moviesSelectionMode){
                    setMoviesSelected(new Set())
                  }
                }}
              >
                Select{moviesSelectionMode && moviesSelected.size > 0 ? ` (${moviesSelected.size})` : ''}
              </button>
              <button
                type="button"
                className="filter-delete-button"
                onClick={async () => {
                  if(moviesSelected.size === 0 || moviesDeleting) return
                  if(!confirm(`Are you sure you want to delete ${moviesSelected.size} item(s)? This action cannot be undone.`)) return
                  
                  // Prevent multiple simultaneous deletions
                  setMoviesDeleting(true)
                  setMoviesLoading(true)
                  
                  try {
                    // Get unique IDs (Set already ensures uniqueness, but be explicit)
                    const selectedIds = Array.from(moviesSelected)
                    const validIds = selectedIds.filter(id => id !== undefined && id !== null && !isNaN(Number(id)))
                    
                    if(validIds.length === 0) {
                      alert('No valid items selected for deletion.')
                      return
                    }
                    
                    // Remove duplicates (shouldn't happen with Set, but be safe)
                    const uniqueIds = [...new Set(validIds)]
                    
                    console.log(`[Delete] Deleting ${uniqueIds.length} movie(s) with IDs:`, uniqueIds)
                    
                    const deletePromises = uniqueIds.map(id => 
                      deleteMedia('movie', id)
                    )
                    const results = await Promise.all(deletePromises)
                    const failed = results.filter(r => !r.ok)
                    
                    if(failed.length > 0){
                      const errorMessages = failed.map(r => r.error || 'Unknown error').join('\n')
                      alert(`Failed to delete ${failed.length} item(s):\n${errorMessages}`)
                    } else {
                      console.log(`[Delete] Successfully deleted ${uniqueIds.length} movie(s)`)
                      setMoviesSelected(new Set())
                      setMoviesSelectionMode(false)
                      await loadMovies(moviesPage)
                    }
                  } catch (err: any) {
                    console.error('[Delete] Error during deletion:', err)
                    alert(`Error deleting items: ${err?.message || 'Unknown error'}`)
                  } finally {
                    setMoviesLoading(false)
                    setMoviesDeleting(false)
                  }
                }}
                disabled={moviesLoading || moviesDeleting || moviesSelected.size === 0 || !moviesSelectionMode}
              >
                Delete{moviesSelected.size > 0 ? ` (${moviesSelected.size})` : ''}
              </button>
              <button
                type="button"
                className="filter-copy-button"
                onClick={async () => {
                  if(moviesSelected.size === 0 || moviesCopying) return
                  if(!confirm(`Are you sure you want to copy ${moviesSelected.size} item(s)?`)) return
                  
                  // Prevent multiple simultaneous copy operations
                  setMoviesCopying(true)
                  setMoviesLoading(true)
                  
                  try {
                    // Get unique IDs
                    const selectedIds = Array.from(moviesSelected)
                    const validIds = selectedIds.filter(id => id !== undefined && id !== null && !isNaN(Number(id)))
                    
                    if(validIds.length === 0) {
                      alert('No valid items selected for copying.')
                      return
                    }
                    
                    const uniqueIds = [...new Set(validIds)]
                    
                    console.log(`[Copy] Copying ${uniqueIds.length} movie(s) with IDs:`, uniqueIds)
                    
                    const copyPromises = uniqueIds.map(id => 
                      copyMedia('movie', id)
                    )
                    const results = await Promise.all(copyPromises)
                    const failed = results.filter(r => !r.ok)
                    
                    if(failed.length > 0){
                      const errorMessages = failed.map(r => r.error || 'Unknown error').join('\n')
                      alert(`Failed to copy ${failed.length} item(s):\n${errorMessages}`)
                    } else {
                      console.log(`[Copy] Successfully copied ${uniqueIds.length} movie(s)`)
                      setMoviesSelected(new Set())
                      setMoviesSelectionMode(false)
                      await loadMovies(moviesPage)
                    }
                  } catch (err: any) {
                    console.error('[Copy] Error during copying:', err)
                    alert(`Error copying items: ${err?.message || 'Unknown error'}`)
                  } finally {
                    setMoviesLoading(false)
                    setMoviesCopying(false)
                  }
                }}
                disabled={moviesLoading || moviesCopying || moviesSelected.size === 0 || !moviesSelectionMode}
              >
                Copy{moviesSelected.size > 0 ? ` (${moviesSelected.size})` : ''}
              </button>
              <button
                type="button"
                className="filter-pagination-button"
                onClick={goToPreviousMovies}
                disabled={moviesLoading || !canNavigateMoviesPrev}
                aria-label="Previous movies page"
              >
                ‹ Previous
              </button>
              <button
                type="button"
                className="filter-pagination-button"
                onClick={goToNextMovies}
                disabled={moviesLoading || !canNavigateMoviesNext}
                aria-label="Next movies page"
              >
                Next ›
              </button>
            </div>
          </div>
          <div className={`tab-results ${moviesViewTransition === 'entering' ? 'tab-section-entering' : ''}`}>
            {moviesReady && movies.length > 0 ? (
              <div
                key={moviesAnimationKey}
                className={`grid movies-grid grid-cascade ${moviesTransitionType === 'slide' ? `movies-grid-slide-${moviesSlideDirection}` : ''}`}
              >
                {movies.map((m, idx) => (
                  <Card 
                    key={`m-${m.tmdb_id}`} 
                    item={m} 
                    onClick={() => !moviesSelectionMode && navigateToDetail('movie', m.id!)}
                    style={{ animationDelay: `${idx * 60}ms` }}
                    selectionMode={moviesSelectionMode}
                    selected={m.id !== undefined && moviesSelected.has(m.id)}
                    onSelectChange={(selected) => {
                      if(m.id === undefined) return
                      const newSelected = new Set(moviesSelected)
                      if(selected){
                        newSelected.add(m.id)
                      } else {
                        newSelected.delete(m.id)
                      }
                      setMoviesSelected(newSelected)
                    }}
                  />
                ))}
              </div>
            ) : (
              <div className="list-placeholder">
                {moviesLoading ? 'Loading titles…' : 'No titles match your filters. Try adjusting the selections and press Search.'}
              </div>
            )}
            {moviesReady && moviesTotal > LIST_PAGE_SIZE && (
              <div className="pagination-controls" aria-label="Movies pagination">
                <button
                  type="button"
                  onClick={goToPreviousMovies}
                  disabled={moviesLoading || !canNavigateMoviesPrev}
                >
                  ‹ Previous
                </button>
                <button
                  type="button"
                  onClick={goToNextMovies}
                  disabled={moviesLoading || !canNavigateMoviesNext}
                >
                  Next ›
                </button>
              </div>
            )}
          </div>
        </section>
      )}

      {tab==='tv' && (
        <section className="tab-section tab-section-tv">
          <div className="list-filters-wrapper">
            <div className="filter-select-group">
              <select
                className="filter-select"
                value={tvPendingGenre}
                onChange={(e) => setTvPendingGenre(e.target.value)}
                disabled={tvLoading && !tvReady}
              >
                <option value="all">All Genres</option>
                {availableGenres.map(g => (
                  <option key={g} value={g}>{g}</option>
                ))}
              </select>
              <select
                className="filter-select"
                value={tvPendingLanguage}
                onChange={(e) => setTvPendingLanguage(e.target.value)}
                disabled={tvLoading && !tvReady}
              >
                <option value="all">All Languages</option>
                {availableLanguages.map(lang => (
                  <option key={lang} value={lang}>{lang.toUpperCase()}</option>
                ))}
              </select>
              <select
                className="filter-select"
                value={tvPendingSort}
                onChange={(e) => setTvPendingSort(e.target.value)}
                disabled={tvLoading && !tvReady}
              >
                <option value="popularity">Popularity</option>
                <option value="rating">Rating</option>
                <option value="title">Title</option>
                <option value="release_date">Release Date</option>
              </select>
              <button
                type="button"
                className={`filter-apply-button${tvFiltersDirty ? ' active' : ''}`}
                onClick={applyTvFilters}
                disabled={tvLoading}
              >
                Search
              </button>
            </div>
            <div className="filter-pagination">
              <button
                type="button"
                className={`filter-action-button${tvSelectionMode ? ' active' : ''}`}
                onClick={() => {
                  setTvSelectionMode(!tvSelectionMode)
                  if(tvSelectionMode){
                    setTvSelected(new Set())
                  }
                }}
              >
                Select{tvSelectionMode && tvSelected.size > 0 ? ` (${tvSelected.size})` : ''}
              </button>
              <button
                type="button"
                className="filter-delete-button"
                onClick={async () => {
                  if(tvSelected.size === 0 || tvDeleting) return
                  if(!confirm(`Are you sure you want to delete ${tvSelected.size} item(s)? This action cannot be undone.`)) return
                  
                  // Prevent multiple simultaneous deletions
                  setTvDeleting(true)
                  setTvLoading(true)
                  
                  try {
                    // Get unique IDs (Set already ensures uniqueness, but be explicit)
                    const selectedIds = Array.from(tvSelected)
                    const validIds = selectedIds.filter(id => id !== undefined && id !== null && !isNaN(Number(id)))
                    
                    if(validIds.length === 0) {
                      alert('No valid items selected for deletion.')
                      return
                    }
                    
                    // Remove duplicates (shouldn't happen with Set, but be safe)
                    const uniqueIds = [...new Set(validIds)]
                    
                    console.log(`[Delete] Deleting ${uniqueIds.length} TV show(s) with IDs:`, uniqueIds)
                    
                    const deletePromises = uniqueIds.map(id => 
                      deleteMedia('tv', id)
                    )
                    const results = await Promise.all(deletePromises)
                    const failed = results.filter(r => !r.ok)
                    
                    if(failed.length > 0){
                      const errorMessages = failed.map(r => r.error || 'Unknown error').join('\n')
                      alert(`Failed to delete ${failed.length} item(s):\n${errorMessages}`)
                    } else {
                      console.log(`[Delete] Successfully deleted ${uniqueIds.length} TV show(s)`)
                      setTvSelected(new Set())
                      setTvSelectionMode(false)
                      await loadTv(tvPage)
                    }
                  } catch (err: any) {
                    console.error('[Delete] Error during deletion:', err)
                    alert(`Error deleting items: ${err?.message || 'Unknown error'}`)
                  } finally {
                    setTvLoading(false)
                    setTvDeleting(false)
                  }
                }}
                disabled={tvLoading || tvDeleting || tvSelected.size === 0 || !tvSelectionMode}
              >
                Delete{tvSelected.size > 0 ? ` (${tvSelected.size})` : ''}
              </button>
              <button
                type="button"
                className="filter-copy-button"
                onClick={async () => {
                  if(tvSelected.size === 0 || tvCopying) return
                  if(!confirm(`Are you sure you want to copy ${tvSelected.size} item(s)?`)) return
                  
                  // Prevent multiple simultaneous copy operations
                  setTvCopying(true)
                  setTvLoading(true)
                  
                  try {
                    // Get unique IDs
                    const selectedIds = Array.from(tvSelected)
                    const validIds = selectedIds.filter(id => id !== undefined && id !== null && !isNaN(Number(id)))
                    
                    if(validIds.length === 0) {
                      alert('No valid items selected for copying.')
                      return
                    }
                    
                    const uniqueIds = [...new Set(validIds)]
                    
                    console.log(`[Copy] Copying ${uniqueIds.length} TV show(s) with IDs:`, uniqueIds)
                    
                    const copyPromises = uniqueIds.map(id => 
                      copyMedia('tv', id)
                    )
                    const results = await Promise.all(copyPromises)
                    const failed = results.filter(r => !r.ok)
                    
                    if(failed.length > 0){
                      const errorMessages = failed.map(r => r.error || 'Unknown error').join('\n')
                      alert(`Failed to copy ${failed.length} item(s):\n${errorMessages}`)
                    } else {
                      console.log(`[Copy] Successfully copied ${uniqueIds.length} TV show(s)`)
                      setTvSelected(new Set())
                      setTvSelectionMode(false)
                      await loadTv(tvPage)
                    }
                  } catch (err: any) {
                    console.error('[Copy] Error during copying:', err)
                    alert(`Error copying items: ${err?.message || 'Unknown error'}`)
                  } finally {
                    setTvLoading(false)
                    setTvCopying(false)
                  }
                }}
                disabled={tvLoading || tvCopying || tvSelected.size === 0 || !tvSelectionMode}
              >
                Copy{tvSelected.size > 0 ? ` (${tvSelected.size})` : ''}
              </button>
              <button
                type="button"
                className="filter-pagination-button"
                onClick={goToPreviousTv}
                disabled={tvLoading || !canNavigateTvPrev}
                aria-label="Previous TV page"
              >
                ‹ Previous
              </button>
              <button
                type="button"
                className="filter-pagination-button"
                onClick={goToNextTv}
                disabled={tvLoading || !canNavigateTvNext}
                aria-label="Next TV page"
              >
                Next ›
              </button>
            </div>
          </div>
          <div className={`tab-results ${tvViewTransition === 'entering' ? 'tab-section-entering' : ''}`}>
            {tvReady && tv.length > 0 ? (
              <div
                key={tvAnimationKey}
                className={`grid tv-grid grid-cascade ${tvTransitionType === 'slide' ? `tv-grid-slide-${tvSlideDirection}` : ''}`}
              >
                {tv.map((m, idx) => (
                  <Card 
                    key={`t-${m.tmdb_id}`} 
                    item={m} 
                    onClick={() => !tvSelectionMode && navigateToDetail('tv', m.id!)}
                    style={{ animationDelay: `${idx * 60}ms` }}
                    selectionMode={tvSelectionMode}
                    selected={m.id !== undefined && tvSelected.has(m.id)}
                    onSelectChange={(selected) => {
                      if(m.id === undefined) return
                      const newSelected = new Set(tvSelected)
                      if(selected){
                        newSelected.add(m.id)
                      } else {
                        newSelected.delete(m.id)
                      }
                      setTvSelected(newSelected)
                    }}
                  />
                ))}
              </div>
            ) : (
              <div className="list-placeholder">
                {tvLoading ? 'Loading titles…' : 'No titles match your filters. Try adjusting the selections and press Search.'}
              </div>
            )}
            {tvReady && tvTotal > LIST_PAGE_SIZE && (
              <div className="pagination-controls" aria-label="TV pagination">
                <button
                  type="button"
                  onClick={goToPreviousTv}
                  disabled={tvLoading || !canNavigateTvPrev}
                >
                  ‹ Previous
                </button>
                <button
                  type="button"
                  onClick={goToNextTv}
                  disabled={tvLoading || !canNavigateTvNext}
                >
                  Next ›
                </button>
              </div>
            )}
          </div>
        </section>
      )}

      {tab==='search' && (
        <section>
          <h3>Search Results{q ? ` for "${q}"` : ''}</h3>
          {results.length === 0 && !busy ? (
            <div style={{textAlign:'center', padding:'48px 24px', color:'var(--muted)'}}>
              No results found. Try a different search term.
            </div>
          ) : (
            <div className="grid">
              {results.map(m => (
                <Card 
                  key={`s-${m.media_type}-${m.tmdb_id}`} 
                  item={m} 
                  onClick={() => navigateToDetail(m.media_type, m.id!)}
                />
              ))}
            </div>
          )}
        </section>
      )}

      {busy && <div className="busy">Loading…</div>}
    </div>
  )
}
