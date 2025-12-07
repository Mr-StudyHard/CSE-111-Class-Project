import { useEffect, useMemo, useRef, useState, type CSSProperties } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import './App.css'
import { getSummary, getList, refresh, search, type MediaItem, getUsers, type UserRow, getHealth, login, signup, getTrending, type TrendingItem, getNewReleases, getMovieDetail, getShowDetail, type MovieDetail, type ShowDetail, getGenres, getLanguages, createMedia, uploadImage, deleteMedia, copyMedia, updateMedia, type UpdateMediaPayload, getReviews, createReview, type Review, getUserByEmail, setAuthToken, clearAuthToken, getUserSettings, updateUserSettings, type UserSettings, getUserProfile, type UserProfile, deleteUserAccount, addToWatchlist, removeFromWatchlist } from './api'
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
    if (path === '/profile') return 'profile'
    if (path === '/settings') return 'settings'
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
  const [currentUser, setCurrentUser] = useState<{user:string;email:string;user_id?:number;display_name?:string;is_admin?:boolean}|null>(() => {
    try {
      const rawSession = sessionStorage.getItem('currentUser')
      if(rawSession){
        const parsed = JSON.parse(rawSession)
        // Set auth token on initial load if user is logged in
        if(parsed?.user_id && parsed?.email) {
          setAuthToken(parsed.user_id, parsed.email)
        }
        return parsed
      }
    } catch {}
    try {
      const flag = localStorage.getItem('rememberUser') === '1'
      if(!flag) return null
      const raw = localStorage.getItem('currentUser')
      if(raw) {
        const parsed = JSON.parse(raw)
        // Set auth token on initial load if user is logged in
        if(parsed?.user_id && parsed?.email) {
          setAuthToken(parsed.user_id, parsed.email)
        }
        return parsed
      }
      return null
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
  const [isInWatchlist, setIsInWatchlist] = useState(false)
  const [watchlistLoading, setWatchlistLoading] = useState(false)
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
  const [settingsData, setSettingsData] = useState<UserSettings | null>(null)
  const [settingsLoading, setSettingsLoading] = useState(false)
  const [settingsError, setSettingsError] = useState<string | null>(null)
  const [settingsCurrentPassword, setSettingsCurrentPassword] = useState('')
  const [settingsDisplayName, setSettingsDisplayName] = useState('')
  const [settingsNewEmail, setSettingsNewEmail] = useState('')
  const [settingsNewPassword, setSettingsNewPassword] = useState('')
  const [settingsConfirmPassword, setSettingsConfirmPassword] = useState('')
  const [settingsSaving, setSettingsSaving] = useState(false)
  const [settingsSuccess, setSettingsSuccess] = useState<string | null>(null)
  const [deleteAccountPassword, setDeleteAccountPassword] = useState('')
  const [deleteAccountConfirm, setDeleteAccountConfirm] = useState('')
  const [deleteAccountDeleting, setDeleteAccountDeleting] = useState(false)
  const [deleteAccountError, setDeleteAccountError] = useState<string | null>(null)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const [profileData, setProfileData] = useState<UserProfile | null>(null)
  const [profileLoading, setProfileLoading] = useState(false)
  const [profileError, setProfileError] = useState<string | null>(null)
  // Favorites carousel state
  const [movieFavPage, setMovieFavPage] = useState(0)
  const [movieFavSlideDirection, setMovieFavSlideDirection] = useState<'left' | 'right' | 'none'>('none')
  const [movieFavTransitionType, setMovieFavTransitionType] = useState<'slide' | 'none'>('none')
  const [tvFavPage, setTvFavPage] = useState(0)
  const [tvFavSlideDirection, setTvFavSlideDirection] = useState<'left' | 'right' | 'none'>('none')
  const [tvFavTransitionType, setTvFavTransitionType] = useState<'slide' | 'none'>('none')
  const FAVORITES_PAGE_SIZE = 6
  // Watchlist expand/collapse state
  const [movieWatchlistExpanded, setMovieWatchlistExpanded] = useState(false)
  const [tvWatchlistExpanded, setTvWatchlistExpanded] = useState(false)
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
    clearAuthToken()
    try {
      sessionStorage.removeItem('currentUser')
      localStorage.removeItem('currentUser')
      localStorage.removeItem('rememberUser')
    } catch {}
  }

  const avatarInitials = useMemo(() => {
    const name = currentUser?.display_name || currentUser?.user || ''
    if(!name.trim()) return ''
    const parts = name.trim().split(/\s+/).slice(0, 2)
    const letters = parts.map(part => part.charAt(0)?.toUpperCase() ?? '').join('')
    return letters || ''
  }, [currentUser?.display_name, currentUser?.user])

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
    const displayName = currentUser.display_name || currentUser.user?.trim() || 'Signed in'
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
            <button
              type="button"
              className="account-menu-item"
              role="menuitem"
              onClick={() => {
                setAccountMenuOpen(false)
                navigateToView('profile')
              }}
            >
              <span className="menu-icon" aria-hidden="true">👤</span>
              <span className="menu-content">
                <span className="menu-title">Profile</span>
                <span className="menu-subtitle">View your stats & watchlist</span>
              </span>
            </button>
            <button
              type="button"
              className="account-menu-item"
              role="menuitem"
              onClick={() => {
                setAccountMenuOpen(false)
                navigateToView('settings')
              }}
            >
              <span className="menu-icon" aria-hidden="true">⚙️</span>
              <span className="menu-content">
                <span className="menu-title">User Settings</span>
                <span className="menu-subtitle">Change password & email</span>
              </span>
            </button>
            {currentUser?.is_admin && (
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
            )}
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

  // Load user profile when profile view is opened
  useEffect(() => {
    if(view === 'profile' || (view === 'detail' && currentUser)){
      (async () => {
        setProfileLoading(true)
        setProfileError(null)
        try {
          const result = await getUserProfile()
          if(result.ok){
            setProfileData(result as UserProfile)
          } else {
            setProfileError(result.error || 'Failed to load profile')
          }
        } catch (e:any) {
          setProfileError(e?.message || 'Failed to load profile')
        } finally {
          setProfileLoading(false)
        }
      })()
    } else if(view !== 'detail') {
      // Only clear profile data if not on detail page
      setProfileData(null)
      setProfileError(null)
      setProfileLoading(false)
    }
  }, [view, currentUser])

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

  // Reset favorites carousel slide directions
  useEffect(() => {
    if (movieFavSlideDirection !== 'none') {
      const timer = setTimeout(() => {
        setMovieFavSlideDirection('none')
        setMovieFavTransitionType('none')
      }, 400)
      return () => clearTimeout(timer)
    }
  }, [movieFavSlideDirection])

  useEffect(() => {
    if (tvFavSlideDirection !== 'none') {
      const timer = setTimeout(() => {
        setTvFavSlideDirection('none')
        setTvFavTransitionType('none')
      }, 400)
      return () => clearTimeout(timer)
    }
  }, [tvFavSlideDirection])


  // Calculate carousel pages for favorites (always call hooks, even if not in profile view)
  const movieFavorites = profileData?.favorites?.movies || []
  const tvFavorites = profileData?.favorites?.tv || []
  const movieWatchlist = profileData?.watchlist?.movies || []
  const tvWatchlist = profileData?.watchlist?.tv || []
  const movieFavTotalPages = useMemo(() => {
    if(movieFavorites.length === 0) return 0
    return Math.ceil(movieFavorites.length / FAVORITES_PAGE_SIZE)
  }, [movieFavorites.length])

  const displayedMovieFavorites = useMemo(() => {
    if(movieFavorites.length === 0) return []
    const clampedPage = Math.min(movieFavPage, Math.max(0, movieFavTotalPages - 1))
    const start = clampedPage * FAVORITES_PAGE_SIZE
    const end = start + FAVORITES_PAGE_SIZE
    return movieFavorites.slice(start, end)
  }, [movieFavorites, movieFavPage, movieFavTotalPages])

  const tvFavTotalPages = useMemo(() => {
    if(tvFavorites.length === 0) return 0
    return Math.ceil(tvFavorites.length / FAVORITES_PAGE_SIZE)
  }, [tvFavorites.length])

  const displayedTvFavorites = useMemo(() => {
    if(tvFavorites.length === 0) return []
    const clampedPage = Math.min(tvFavPage, Math.max(0, tvFavTotalPages - 1))
    const start = clampedPage * FAVORITES_PAGE_SIZE
    const end = start + FAVORITES_PAGE_SIZE
    return tvFavorites.slice(start, end)
  }, [tvFavorites, tvFavPage, tvFavTotalPages])


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
        } catch (e: any) {
          setAccounts([])
          const status = e?.response?.status
          if(status === 401){
            setAccountsError('Authentication required. Please log in to view accounts.')
          } else if(status === 403){
            setAccountsError('Admin privileges required. Only administrators can view stored accounts.')
          } else {
            setAccountsError('Could not load accounts. Ensure the backend is running and the dev proxy is active.')
            setBackendOnline(false)
          }
        }
      })()
    }
  }, [view])

  // Load user settings when settings view is opened
  useEffect(() => {
    if(view === 'settings'){
      (async () => {
        setSettingsLoading(true)
        setSettingsError(null)
        setSettingsSuccess(null)
        try {
          const result = await getUserSettings()
          if(result.ok){
            setSettingsData({
              user_id: result.user_id,
              email: result.email,
              display_name: result.display_name,
              created_at: result.created_at,
              is_admin: result.is_admin
            })
            setSettingsDisplayName(result.display_name || result.email.split('@')[0] || '')
            setSettingsNewEmail(result.email)
          } else {
            setSettingsError(result.error || 'Failed to load settings')
          }
        } catch (e: any) {
          setSettingsError(e?.message || 'Failed to load settings')
        } finally {
          setSettingsLoading(false)
        }
      })()
    } else {
      // Reset form when leaving settings view
      setSettingsCurrentPassword('')
      setSettingsDisplayName('')
      setSettingsNewEmail('')
      setSettingsNewPassword('')
      setSettingsConfirmPassword('')
      setSettingsSuccess(null)
      setSettingsError(null)
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

  // Check watchlist status when detail data or profile data changes
  useEffect(() => {
    if (detailData && currentUser && profileData) {
      const mediaType = detailData.media_type
      const id = mediaType === 'movie' 
        ? (detailData as MovieDetail).movie_id 
        : (detailData as ShowDetail).show_id
      
      if (mediaType === 'movie') {
        const inWatchlist = profileData.watchlist.movies.some(m => m.id === id)
        setIsInWatchlist(inWatchlist)
      } else {
        const inWatchlist = profileData.watchlist.tv.some(t => t.id === id)
        setIsInWatchlist(inWatchlist)
      }
    } else {
      setIsInWatchlist(false)
    }
  }, [detailData, currentUser, profileData])

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
          const profile = { 
            user: (res as any).display_name || (res as any).user, 
            email: (res as any).email, 
            user_id: (res as any).user_id,
            display_name: (res as any).display_name || (res as any).user,
            is_admin: (res as any).is_admin || false
          }
          setCurrentUser(profile)
          // Set auth token for API requests
          setAuthToken(profile.user_id, profile.email)
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
                clearAuthToken();
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
            <h1 className="auth-landing-title">PlotSignal</h1>
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

  if(view === 'profile'){
    if(!currentUser){
      return (
        <div className="container">
          <header className="header">
            <div className="header-left">
              <div className="brand-group">
                <button type="button" className="brand-link" onClick={()=>{ navigateToTab('home'); }}>
                  PlotSignal
                </button>
              </div>
            </div>
            <div className="header-right">
              <div className="header-actions">
                {renderAccountControls()}
              </div>
            </div>
          </header>
          <section className="settings-layout">
            <div className="card" style={{padding:'32px', background:'rgba(20,20,28,0.95)', border:'1px solid rgba(255,255,255,0.08)', borderRadius:'16px', textAlign:'center'}}>
              <h1 className="form-title" style={{marginBottom:12, color:'var(--text)'}}>Profile</h1>
              <p style={{textAlign:'center', color:'var(--muted)', marginBottom:'24px'}}>
                Please log in to view your profile and stats.
              </p>
              <button className="btn-primary" onClick={() => navigateToView('login')} style={{width:'100%'}}>
                Go to Login
              </button>
              <div className="auth-card-footer" style={{marginTop:'20px'}}>
                <button className="back-link-button" onClick={()=>navigateToView('app')}>← Back to app</button>
              </div>
            </div>
          </section>
        </div>
      )
    }

    const stats = profileData?.stats
    const userInfo = profileData?.user
    const displayName = userInfo?.display_name || currentUser?.display_name || currentUser?.user || 'User'

    return (
      <div className="container">
        <header className="header">
          <div className="header-left">
            <div className="brand-group">
              <button type="button" className="brand-link" onClick={()=>{ navigateToTab('home'); }}>
                PlotSignal
              </button>
            </div>
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

        {/* Profile Hero Section */}
        <div style={{
          background:'linear-gradient(135deg, rgba(229, 9, 20, 0.15) 0%, rgba(11, 11, 15, 0.95) 100%)',
          border:'1px solid rgba(229, 9, 20, 0.25)',
          borderRadius:'20px',
          padding:'40px 32px',
          marginBottom:'32px',
          position:'relative',
          overflow:'hidden'
        }}>
          <div style={{position:'relative', zIndex:1, display:'flex', alignItems:'center', gap:'24px'}}>
            <div className="avatar-circle" style={{width:80, height:80, fontSize:'32px'}}>
              <span className="avatar-initials">
                {displayName.split(/\s+/).slice(0,2).map(n=>n.charAt(0).toUpperCase()).join('') || '👤'}
              </span>
            </div>
            <div style={{flex:1}}>
              <h1 style={{margin:0, fontSize:'36px', fontWeight:800, color:'var(--text)', marginBottom:'8px'}}>
                {displayName}
              </h1>
              <div style={{display:'flex', gap:'24px', alignItems:'center', flexWrap:'wrap'}}>
                {userInfo?.email && (
                  <div style={{color:'var(--muted)', fontSize:'14px'}}>{userInfo.email}</div>
                )}
                {userInfo?.created_at && (
                  <div style={{color:'var(--muted)', fontSize:'14px'}}>
                    Member since {new Date(userInfo.created_at).toLocaleDateString('en-US', { month:'long', year:'numeric' })}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Stats Section - Separated by Movies and TV */}
        {profileLoading ? (
          <div style={{textAlign:'center', padding:'60px 20px', color:'var(--muted)'}}>Loading your profile...</div>
        ) : profileError ? (
          <div style={{textAlign:'center', padding:'60px 20px', color:'#ef5350'}}>{profileError}</div>
        ) : stats ? (
          <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:'24px', marginBottom:'40px'}}>
            {/* Movies Stats */}
            <div style={{
              background:'rgba(20, 20, 28, 0.95)',
              border:'1px solid rgba(229, 9, 20, 0.2)',
              borderRadius:'20px',
              padding:'32px',
              position:'relative',
              overflow:'hidden'
            }}>
              <div style={{
                position:'absolute',
                top:0,
                right:0,
                width:200,
                height:200,
                background:'radial-gradient(circle, rgba(229, 9, 20, 0.15) 0%, transparent 70%)',
                opacity:0.6
              }}></div>
              <div style={{position:'relative', zIndex:1}}>
                <div style={{display:'flex', alignItems:'center', gap:'12px', marginBottom:'24px'}}>
                  <div style={{fontSize:'24px'}}>🎬</div>
                  <h2 style={{margin:0, fontSize:'22px', fontWeight:700, color:'var(--text)'}}>Movies</h2>
                </div>
                <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:'16px'}}>
                  <div>
                    <div style={{fontSize:'12px', color:'var(--muted)', marginBottom:'6px', fontWeight:600, textTransform:'uppercase', letterSpacing:'0.5px'}}>Reviews</div>
                    <div style={{fontSize:'36px', fontWeight:800, color:'var(--text)', lineHeight:1}}>{stats.movies?.review_count || 0}</div>
                  </div>
                  <div>
                    <div style={{fontSize:'12px', color:'var(--muted)', marginBottom:'6px', fontWeight:600, textTransform:'uppercase', letterSpacing:'0.5px'}}>Avg Rating</div>
                    <div style={{fontSize:'36px', fontWeight:800, color:'var(--text)', lineHeight:1}}>
                      {stats.movies?.avg_rating?.toFixed(1) || '0.0'}
                    </div>
                  </div>
                  <div>
                    <div style={{fontSize:'12px', color:'var(--muted)', marginBottom:'6px', fontWeight:600, textTransform:'uppercase', letterSpacing:'0.5px'}}>Hours Watched</div>
                    <div style={{fontSize:'36px', fontWeight:800, color:'var(--text)', lineHeight:1}}>{stats.movies?.estimated_hours || 0}</div>
                  </div>
                  <div>
                    <div style={{fontSize:'12px', color:'var(--muted)', marginBottom:'6px', fontWeight:600, textTransform:'uppercase', letterSpacing:'0.5px'}}>Discussion Posts</div>
                    <div style={{fontSize:'36px', fontWeight:800, color:'var(--text)', lineHeight:1}}>{stats.movies?.discussion_count || 0}</div>
                  </div>
                </div>
              </div>
            </div>

            {/* TV Stats */}
            <div style={{
              background:'rgba(20, 20, 28, 0.95)',
              border:'1px solid rgba(37, 99, 235, 0.2)',
              borderRadius:'20px',
              padding:'32px',
              position:'relative',
              overflow:'hidden'
            }}>
              <div style={{
                position:'absolute',
                top:0,
                right:0,
                width:200,
                height:200,
                background:'radial-gradient(circle, rgba(37, 99, 235, 0.15) 0%, transparent 70%)',
                opacity:0.6
              }}></div>
              <div style={{position:'relative', zIndex:1}}>
                <div style={{display:'flex', alignItems:'center', gap:'12px', marginBottom:'24px'}}>
                  <div style={{fontSize:'24px'}}>📺</div>
                  <h2 style={{margin:0, fontSize:'22px', fontWeight:700, color:'var(--text)'}}>TV Shows</h2>
                </div>
                <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:'16px'}}>
                  <div>
                    <div style={{fontSize:'12px', color:'var(--muted)', marginBottom:'6px', fontWeight:600, textTransform:'uppercase', letterSpacing:'0.5px'}}>Reviews</div>
                    <div style={{fontSize:'36px', fontWeight:800, color:'var(--text)', lineHeight:1}}>{stats.tv?.review_count || 0}</div>
                  </div>
                  <div>
                    <div style={{fontSize:'12px', color:'var(--muted)', marginBottom:'6px', fontWeight:600, textTransform:'uppercase', letterSpacing:'0.5px'}}>Avg Rating</div>
                    <div style={{fontSize:'36px', fontWeight:800, color:'var(--text)', lineHeight:1}}>
                      {stats.tv?.avg_rating?.toFixed(1) || '0.0'}
                    </div>
                  </div>
                  <div>
                    <div style={{fontSize:'12px', color:'var(--muted)', marginBottom:'6px', fontWeight:600, textTransform:'uppercase', letterSpacing:'0.5px'}}>Hours Watched</div>
                    <div style={{fontSize:'36px', fontWeight:800, color:'var(--text)', lineHeight:1}}>{stats.tv?.estimated_hours || 0}</div>
                  </div>
                  <div>
                    <div style={{fontSize:'12px', color:'var(--muted)', marginBottom:'6px', fontWeight:600, textTransform:'uppercase', letterSpacing:'0.5px'}}>Discussion Posts</div>
                    <div style={{fontSize:'36px', fontWeight:800, color:'var(--text)', lineHeight:1}}>{stats.tv?.discussion_count || 0}</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        ) : null}

        {/* Favorites Section - Carousels stacked vertically */}
        <div style={{marginBottom:'40px'}}>
          <h2 style={{margin:0, marginBottom:'32px', fontSize:'24px', fontWeight:700, color:'var(--text)'}}>Your Favorites</h2>
          
          {/* Movie Favorites Carousel */}
          <div style={{marginBottom:'40px'}}>
            <div style={{display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:'20px'}}>
              <h3 style={{margin:0, fontSize:'20px', fontWeight:600, color:'var(--text)', display:'flex', alignItems:'center', gap:'10px'}}>
                <span style={{fontSize:'24px'}}>🎬</span> Movies
              </h3>
              {movieFavorites.length > 0 && (
                <div style={{fontSize:'14px', color:'var(--muted)'}}>
                  {movieFavorites.length} {movieFavorites.length === 1 ? 'item' : 'items'}
                  {movieFavTotalPages > 1 && ` • Page ${movieFavPage + 1} of ${movieFavTotalPages}`}
                </div>
              )}
            </div>
            {profileLoading ? (
              <p style={{color:'var(--muted)', textAlign:'center', padding:'40px'}}>Loading...</p>
            ) : movieFavorites.length === 0 ? (
              <div style={{
                padding:'60px 20px',
                textAlign:'center',
                background:'rgba(20, 20, 28, 0.95)',
                border:'1px solid rgba(255, 255, 255, 0.08)',
                borderRadius:'16px'
              }}>
                <div style={{fontSize:'48px', marginBottom:'16px', opacity:0.5}}>🎬</div>
                <p style={{color:'var(--muted)', fontSize:'16px', margin:0}}>No movie favorites yet. Start rating movies!</p>
              </div>
            ) : (
              <>
                <div className={`new-release-grid-wrapper ${movieFavTransitionType === 'fade' ? `new-release-grid-wrapper-${'visible'}` : ''}`} style={{position:'relative', minHeight:'300px'}}>
                  <div className={`new-release-grid ${movieFavTransitionType === 'slide' ? `new-release-grid-slide-${movieFavSlideDirection}` : ''}`} style={{display:'grid', gap:'14px', gridTemplateColumns:'repeat(auto-fill, minmax(180px, 1fr))', alignContent:'start'}}>
                    {displayedMovieFavorites.map((fav, idx) => {
                      const posterUrl = getImageUrl(fav.poster_path, 'w300')
                      return (
                        <Card
                          key={`movie-fav-${fav.id || idx}`}
                          item={{
                            id: fav.id,
                            tmdb_id: fav.id || 0,
                            media_type: 'movie',
                            title: fav.title,
                            poster_path: fav.poster_path || undefined,
                            vote_average: fav.rating
                          } as MediaItem}
                          onClick={() => {
                            if(fav.id) {
                              navigateToDetail('movie', fav.id)
                            }
                          }}
                        />
                      )
                    })}
                  </div>
                </div>
                {movieFavTotalPages > 1 && (
                  <div className="pagination-controls" style={{marginTop:'20px'}} aria-label="Movie favorites pagination">
                    <button
                      type="button"
                      onClick={() => {
                        setMovieFavTransitionType('slide')
                        setMovieFavSlideDirection('right')
                        setMovieFavPage(prev => Math.max(0, prev - 1))
                      }}
                      disabled={profileLoading || movieFavPage === 0}
                    >
                      ‹ Previous
                    </button>
                    <button
                      type="button"
                      onClick={() => {
                        setMovieFavTransitionType('slide')
                        setMovieFavSlideDirection('left')
                        setMovieFavPage(prev => Math.min(movieFavTotalPages - 1, prev + 1))
                      }}
                      disabled={profileLoading || movieFavPage >= movieFavTotalPages - 1}
                    >
                      Next ›
                    </button>
                  </div>
                )}
              </>
            )}
          </div>

          {/* TV Favorites Carousel */}
          <div>
            <div style={{display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:'20px'}}>
              <h3 style={{margin:0, fontSize:'20px', fontWeight:600, color:'var(--text)', display:'flex', alignItems:'center', gap:'10px'}}>
                <span style={{fontSize:'24px'}}>📺</span> TV Shows
              </h3>
              {tvFavorites.length > 0 && (
                <div style={{fontSize:'14px', color:'var(--muted)'}}>
                  {tvFavorites.length} {tvFavorites.length === 1 ? 'item' : 'items'}
                  {tvFavTotalPages > 1 && ` • Page ${tvFavPage + 1} of ${tvFavTotalPages}`}
                </div>
              )}
            </div>
            {profileLoading ? (
              <p style={{color:'var(--muted)', textAlign:'center', padding:'40px'}}>Loading...</p>
            ) : tvFavorites.length === 0 ? (
              <div style={{
                padding:'60px 20px',
                textAlign:'center',
                background:'rgba(20, 20, 28, 0.95)',
                border:'1px solid rgba(255, 255, 255, 0.08)',
                borderRadius:'16px'
              }}>
                <div style={{fontSize:'48px', marginBottom:'16px', opacity:0.5}}>📺</div>
                <p style={{color:'var(--muted)', fontSize:'16px', margin:0}}>No TV favorites yet. Start rating shows!</p>
              </div>
            ) : (
              <>
                <div className={`new-release-grid-wrapper ${tvFavTransitionType === 'fade' ? `new-release-grid-wrapper-${'visible'}` : ''}`} style={{position:'relative', minHeight:'300px'}}>
                  <div className={`new-release-grid ${tvFavTransitionType === 'slide' ? `new-release-grid-slide-${tvFavSlideDirection}` : ''}`} style={{display:'grid', gap:'14px', gridTemplateColumns:'repeat(auto-fill, minmax(180px, 1fr))', alignContent:'start'}}>
                    {displayedTvFavorites.map((fav, idx) => {
                      const posterUrl = getImageUrl(fav.poster_path, 'w300')
                      return (
                        <Card
                          key={`tv-fav-${fav.id || idx}`}
                          item={{
                            id: fav.id,
                            tmdb_id: fav.id || 0,
                            media_type: 'tv',
                            title: fav.title,
                            poster_path: fav.poster_path || undefined,
                            vote_average: fav.rating
                          } as MediaItem}
                          onClick={() => {
                            if(fav.id) {
                              navigateToDetail('tv', fav.id)
                            }
                          }}
                        />
                      )
                    })}
                  </div>
                </div>
                {tvFavTotalPages > 1 && (
                  <div className="pagination-controls" style={{marginTop:'20px'}} aria-label="TV favorites pagination">
                    <button
                      type="button"
                      onClick={() => {
                        setTvFavTransitionType('slide')
                        setTvFavSlideDirection('right')
                        setTvFavPage(prev => Math.max(0, prev - 1))
                      }}
                      disabled={profileLoading || tvFavPage === 0}
                    >
                      ‹ Previous
                    </button>
                    <button
                      type="button"
                      onClick={() => {
                        setTvFavTransitionType('slide')
                        setTvFavSlideDirection('left')
                        setTvFavPage(prev => Math.min(tvFavTotalPages - 1, prev + 1))
                      }}
                      disabled={profileLoading || tvFavPage >= tvFavTotalPages - 1}
                    >
                      Next ›
                    </button>
                  </div>
                )}
              </>
            )}
          </div>
        </div>


        {/* Watchlist Section - Horizontal Scrollable Sections */}
        <div>
          <h2 style={{margin:0, marginBottom:'32px', fontSize:'24px', fontWeight:700, color:'var(--text)'}}>My Watchlist</h2>
          
          {/* Movie Watchlist - Red Theme */}
          <div style={{
            marginBottom:'40px',
            padding:'24px',
            background:'linear-gradient(135deg, rgba(229, 9, 20, 0.08) 0%, rgba(184, 7, 15, 0.04) 100%)',
            border:'2px solid rgba(229, 9, 20, 0.3)',
            borderRadius:'20px',
            boxShadow:'0 8px 32px rgba(229, 9, 20, 0.12)',
            transition:'all 0.3s ease'
          }}>
            <div style={{display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:'20px'}}>
              <h3 style={{
                margin:0, 
                fontSize:'22px', 
                fontWeight:700, 
                color:'#fff', 
                display:'flex', 
                alignItems:'center', 
                gap:'12px',
                textShadow:'0 2px 8px rgba(229, 9, 20, 0.4)'
              }}>
                <span style={{
                  fontSize:'28px',
                  filter:'drop-shadow(0 2px 4px rgba(229, 9, 20, 0.5))'
                }}>🎬</span> 
                <span style={{
                  background:'linear-gradient(135deg, #ff6b6b 0%, #e50914 100%)',
                  WebkitBackgroundClip:'text',
                  WebkitTextFillColor:'transparent',
                  backgroundClip:'text'
                }}>Movies</span>
              </h3>
              <div style={{display:'flex', alignItems:'center', gap:'12px'}}>
                {movieWatchlist.length > 0 && (
                  <div style={{
                    fontSize:'14px', 
                    color:'rgba(255, 255, 255, 0.8)',
                    background:'rgba(229, 9, 20, 0.2)',
                    padding:'6px 14px',
                    borderRadius:'12px',
                    border:'1px solid rgba(229, 9, 20, 0.3)'
                  }}>
                    {movieWatchlist.length} {movieWatchlist.length === 1 ? 'item' : 'items'}
                  </div>
                )}
                {movieWatchlist.length > 0 && (
                  <button
                    type="button"
                    onClick={() => setMovieWatchlistExpanded(!movieWatchlistExpanded)}
                    style={{
                      background:'rgba(229, 9, 20, 0.2)',
                      border:'1px solid rgba(229, 9, 20, 0.4)',
                      borderRadius:'10px',
                      width:'40px',
                      height:'40px',
                      display:'flex',
                      alignItems:'center',
                      justifyContent:'center',
                      cursor:'pointer',
                      transition:'all 0.3s ease',
                      color:'#fff',
                      fontSize:'18px',
                      padding:0
                    }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.background = 'rgba(229, 9, 20, 0.35)'
                      e.currentTarget.style.borderColor = 'rgba(229, 9, 20, 0.6)'
                      e.currentTarget.style.transform = 'scale(1.05)'
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.background = 'rgba(229, 9, 20, 0.2)'
                      e.currentTarget.style.borderColor = 'rgba(229, 9, 20, 0.4)'
                      e.currentTarget.style.transform = 'scale(1)'
                    }}
                    aria-label={movieWatchlistExpanded ? 'Collapse movies' : 'Expand movies'}
                  >
                    <span style={{
                      display:'inline-block',
                      transform: movieWatchlistExpanded ? 'rotate(180deg)' : 'rotate(0deg)',
                      transition:'transform 0.3s ease'
                    }}>▼</span>
                  </button>
                )}
              </div>
            </div>
            {profileLoading ? (
              <p style={{color:'var(--muted)', textAlign:'center', padding:'40px'}}>Loading...</p>
            ) : movieWatchlist.length === 0 ? (
              <div style={{
                padding:'60px 20px',
                textAlign:'center',
                background:'rgba(20, 20, 28, 0.6)',
                border:'1px solid rgba(229, 9, 20, 0.3)',
                borderRadius:'16px'
              }}>
                <div style={{fontSize:'48px', marginBottom:'16px', opacity:0.5}}>🎬</div>
                <p style={{color:'rgba(255, 255, 255, 0.7)', fontSize:'16px', margin:0}}>No movies in your watchlist. Add movies to watch later!</p>
              </div>
            ) : (
              <div style={{
                maxHeight: movieWatchlistExpanded ? '600px' : '0',
                overflow:'hidden',
                transition:'max-height 0.4s cubic-bezier(0.4, 0, 0.2, 1), opacity 0.3s ease',
                opacity: movieWatchlistExpanded ? 1 : 0
              }}>
                <div style={{
                  display:'grid',
                  gridTemplateColumns:'repeat(auto-fill, minmax(180px, 1fr))',
                  gap:'16px',
                  maxHeight:'600px',
                  overflowY:'auto',
                  overflowX:'hidden',
                  paddingRight:'8px',
                  paddingTop:'8px',
                  scrollbarWidth:'thin',
                  scrollbarColor:'rgba(229, 9, 20, 0.5) rgba(20, 20, 28, 0.3)',
                  WebkitOverflowScrolling:'touch'
                }}>
                  {movieWatchlist.map((item, idx) => {
                    return (
                      <Card
                        key={`movie-watchlist-${item.id || idx}`}
                        item={{
                          id: item.id,
                          tmdb_id: item.id || 0,
                          media_type: 'movie',
                          title: item.title,
                          poster_path: item.poster_path || undefined,
                        } as MediaItem}
                        onClick={() => {
                          if(item.id) {
                            navigateToDetail('movie', item.id)
                          }
                        }}
                      />
                    )
                  })}
                </div>
              </div>
            )}
          </div>

          {/* TV Watchlist - Blue Theme */}
          <div style={{
            padding:'24px',
            background:'linear-gradient(135deg, rgba(37, 99, 235, 0.08) 0%, rgba(30, 64, 175, 0.04) 100%)',
            border:'2px solid rgba(37, 99, 235, 0.3)',
            borderRadius:'20px',
            boxShadow:'0 8px 32px rgba(37, 99, 235, 0.12)',
            transition:'all 0.3s ease'
          }}>
            <div style={{display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:'20px'}}>
              <h3 style={{
                margin:0, 
                fontSize:'22px', 
                fontWeight:700, 
                color:'#fff', 
                display:'flex', 
                alignItems:'center', 
                gap:'12px',
                textShadow:'0 2px 8px rgba(37, 99, 235, 0.4)'
              }}>
                <span style={{
                  fontSize:'28px',
                  filter:'drop-shadow(0 2px 4px rgba(37, 99, 235, 0.5))'
                }}>📺</span> 
                <span style={{
                  background:'linear-gradient(135deg, #60a5fa 0%, #2563eb 100%)',
                  WebkitBackgroundClip:'text',
                  WebkitTextFillColor:'transparent',
                  backgroundClip:'text'
                }}>TV Shows</span>
              </h3>
              <div style={{display:'flex', alignItems:'center', gap:'12px'}}>
                {tvWatchlist.length > 0 && (
                  <div style={{
                    fontSize:'14px', 
                    color:'rgba(255, 255, 255, 0.8)',
                    background:'rgba(37, 99, 235, 0.2)',
                    padding:'6px 14px',
                    borderRadius:'12px',
                    border:'1px solid rgba(37, 99, 235, 0.3)'
                  }}>
                    {tvWatchlist.length} {tvWatchlist.length === 1 ? 'item' : 'items'}
                  </div>
                )}
                {tvWatchlist.length > 0 && (
                  <button
                    type="button"
                    onClick={() => setTvWatchlistExpanded(!tvWatchlistExpanded)}
                    style={{
                      background:'rgba(37, 99, 235, 0.2)',
                      border:'1px solid rgba(37, 99, 235, 0.4)',
                      borderRadius:'10px',
                      width:'40px',
                      height:'40px',
                      display:'flex',
                      alignItems:'center',
                      justifyContent:'center',
                      cursor:'pointer',
                      transition:'all 0.3s ease',
                      color:'#fff',
                      fontSize:'18px',
                      padding:0
                    }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.background = 'rgba(37, 99, 235, 0.35)'
                      e.currentTarget.style.borderColor = 'rgba(37, 99, 235, 0.6)'
                      e.currentTarget.style.transform = 'scale(1.05)'
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.background = 'rgba(37, 99, 235, 0.2)'
                      e.currentTarget.style.borderColor = 'rgba(37, 99, 235, 0.4)'
                      e.currentTarget.style.transform = 'scale(1)'
                    }}
                    aria-label={tvWatchlistExpanded ? 'Collapse TV shows' : 'Expand TV shows'}
                  >
                    <span style={{
                      display:'inline-block',
                      transform: tvWatchlistExpanded ? 'rotate(180deg)' : 'rotate(0deg)',
                      transition:'transform 0.3s ease'
                    }}>▼</span>
                  </button>
                )}
              </div>
            </div>
            {profileLoading ? (
              <p style={{color:'var(--muted)', textAlign:'center', padding:'40px'}}>Loading...</p>
            ) : tvWatchlist.length === 0 ? (
              <div style={{
                padding:'60px 20px',
                textAlign:'center',
                background:'rgba(20, 20, 28, 0.6)',
                border:'1px solid rgba(37, 99, 235, 0.3)',
                borderRadius:'16px'
              }}>
                <div style={{fontSize:'48px', marginBottom:'16px', opacity:0.5}}>📺</div>
                <p style={{color:'rgba(255, 255, 255, 0.7)', fontSize:'16px', margin:0}}>No TV shows in your watchlist. Add shows to watch later!</p>
              </div>
            ) : (
              <div style={{
                maxHeight: tvWatchlistExpanded ? '600px' : '0',
                overflow:'hidden',
                transition:'max-height 0.4s cubic-bezier(0.4, 0, 0.2, 1), opacity 0.3s ease',
                opacity: tvWatchlistExpanded ? 1 : 0
              }}>
                <div style={{
                  display:'grid',
                  gridTemplateColumns:'repeat(auto-fill, minmax(180px, 1fr))',
                  gap:'16px',
                  maxHeight:'600px',
                  overflowY:'auto',
                  overflowX:'hidden',
                  paddingRight:'8px',
                  paddingTop:'8px',
                  scrollbarWidth:'thin',
                  scrollbarColor:'rgba(37, 99, 235, 0.5) rgba(20, 20, 28, 0.3)',
                  WebkitOverflowScrolling:'touch'
                }}>
                  {tvWatchlist.map((item, idx) => {
                    return (
                      <Card
                        key={`tv-watchlist-${item.id || idx}`}
                        item={{
                          id: item.id,
                          tmdb_id: item.id || 0,
                          media_type: 'tv',
                          title: item.title,
                          poster_path: item.poster_path || undefined,
                        } as MediaItem}
                        onClick={() => {
                          if(item.id) {
                            navigateToDetail('tv', item.id)
                          }
                        }}
                      />
                    )
                  })}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    )
  }

  if(view === 'settings'){
    if(!currentUser){
      return (
        <div className="container">
          <header className="header">
            <div className="header-left">
              <div className="brand-group">
                <button type="button" className="brand-link" onClick={()=>{ navigateToTab('home'); }}>
                  PlotSignal
                </button>
              </div>
            </div>
            <div className="header-right">
              <div className="header-actions">
                {renderAccountControls()}
              </div>
            </div>
          </header>
          <section className="settings-layout">
            <div className="card" style={{
              padding:'32px',
              background:'rgba(20, 20, 28, 0.95)',
              border:'1px solid rgba(255, 255, 255, 0.08)',
              borderRadius:'16px',
              textAlign:'center'
            }}>
              <h1 className="form-title" style={{marginBottom:12, color:'var(--text)'}}>User Settings</h1>
              <p style={{textAlign:'center', color:'var(--muted)', marginBottom:'24px'}}>
                Please log in to access your account settings.
              </p>
              <button className="btn-primary" onClick={() => navigateToView('login')} style={{width:'100%'}}>
                Go to Login
              </button>
              <div className="auth-card-footer" style={{marginTop:'20px'}}>
                <button className="back-link-button" onClick={()=>navigateToView('app')}>← Back to app</button>
              </div>
            </div>
          </section>
        </div>
      )
    }

    const getPasswordStrength = (password: string) => {
      if(!password) return { score: 0, label: '', color: 'rgba(255, 255, 255, 0.2)' }
      let score = 0
      if(password.length >= 8) score++
      if(/[a-z]/.test(password) && /[A-Z]/.test(password)) score++
      if(/\d/.test(password)) score++
      if(/[^a-zA-Z0-9]/.test(password)) score++
      
      if(score <= 1) return { score, label: 'Weak', color: '#ef5350' }
      if(score === 2) return { score, label: 'Fair', color: '#ffa726' }
      if(score === 3) return { score, label: 'Good', color: '#66bb6a' }
      return { score, label: 'Strong', color: '#81c784' }
    }

    const passwordStrength = getPasswordStrength(settingsNewPassword)

    const handleSettingsSubmit = async (ev: React.FormEvent) => {
      ev.preventDefault()
      setSettingsError(null)
      setSettingsSuccess(null)

      if(!settingsCurrentPassword){
        setSettingsError('Current password is required to make changes')
        return
      }

      if(settingsNewPassword && settingsNewPassword !== settingsConfirmPassword){
        setSettingsError('New passwords do not match')
        return
      }

      const currentDisplayName = settingsData?.display_name || settingsData?.email?.split('@')[0] || ''
      const hasDisplayNameChange = settingsDisplayName && settingsDisplayName.trim() !== currentDisplayName.trim()
      const hasEmailChange = settingsNewEmail && settingsNewEmail !== settingsData?.email
      const hasPasswordChange = settingsNewPassword && settingsNewPassword.length > 0

      if(!hasDisplayNameChange && !hasEmailChange && !hasPasswordChange){
        setSettingsError('Please make at least one change before saving')
        return
      }

      if(settingsDisplayName && settingsDisplayName.trim().length > 50){
        setSettingsError('Display name must be 50 characters or less')
        return
      }

      setSettingsSaving(true)

      try {
        const result = await updateUserSettings({
          current_password: settingsCurrentPassword,
          display_name: hasDisplayNameChange ? settingsDisplayName.trim() : undefined,
          new_email: hasEmailChange ? settingsNewEmail : undefined,
          new_password: hasPasswordChange ? settingsNewPassword : undefined
        })

        if(result.ok){
          setSettingsSuccess(result.message || 'Your settings have been updated.')
          setSettingsCurrentPassword('')
          setSettingsNewPassword('')
          setSettingsConfirmPassword('')
          
          // Update current user state if display_name or email changed
          if(hasDisplayNameChange || hasEmailChange){
            const updatedUser = {
              ...currentUser,
              email: hasEmailChange ? settingsNewEmail : currentUser?.email,
              user: hasDisplayNameChange ? settingsDisplayName.trim() : (hasEmailChange ? settingsNewEmail.split('@')[0] : currentUser?.user),
              display_name: hasDisplayNameChange ? settingsDisplayName.trim() : currentUser?.display_name
            }
            setCurrentUser(updatedUser)
            if(hasEmailChange && updatedUser.user_id && updatedUser.email){
              setAuthToken(updatedUser.user_id, updatedUser.email)
            }
            try {
              sessionStorage.setItem('currentUser', JSON.stringify(updatedUser))
              if(remember){
                localStorage.setItem('currentUser', JSON.stringify(updatedUser))
              }
            } catch {}
          }
          
          const refreshResult = await getUserSettings()
          if(refreshResult.ok){
            setSettingsData({
              user_id: refreshResult.user_id,
              email: refreshResult.email,
              display_name: refreshResult.display_name,
              created_at: refreshResult.created_at,
              is_admin: refreshResult.is_admin
            })
            setSettingsDisplayName(refreshResult.display_name || refreshResult.email.split('@')[0] || '')
            setSettingsNewEmail(refreshResult.email)
          }
        } else {
          setSettingsError(result.error || 'Failed to update settings')
        }
      } catch (e: any) {
        setSettingsError(e?.message || 'An unexpected error occurred. Please try again.')
      } finally {
        setSettingsSaving(false)
      }
    }

    return (
      <div className="container">
        <header className="header">
          <div className="header-left">
            <div className="brand-group">
              <button type="button" className="brand-link" onClick={()=>{ navigateToTab('home'); }}>
                PlotSignal
              </button>
            </div>
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

        <section className="settings-layout" style={{display:'grid', gridTemplateColumns:'1fr 1.4fr', gap:'20px', alignItems:'start'}}>
          <div style={{display:'flex', flexDirection:'column', gap:'20px'}}>
            {/* Account Info Card */}
            <div className="card" style={{
              padding:'24px',
              background:'rgba(20, 20, 28, 0.95)',
              border:'1px solid rgba(255, 255, 255, 0.08)',
              borderRadius:'16px'
            }}>
              <div style={{display:'flex', alignItems:'center', gap:'14px', marginBottom:'20px'}}>
                <div className="avatar-circle" style={{width:52, height:52}}>
                  <span className="avatar-initials">
                    {settingsData?.display_name 
                      ? settingsData.display_name.split(/\s+/).slice(0, 2).map(n => n.charAt(0).toUpperCase()).join('')
                      : avatarInitials || '👤'}
                  </span>
                </div>
                <div>
                  <div style={{fontWeight:700, fontSize:'18px', color:'var(--text)'}}>
                    {settingsData?.display_name || currentUser.user}
                  </div>
                  <div style={{color:'var(--muted)', fontSize:'13px', marginTop:'2px'}}>{settingsData?.email}</div>
                </div>
              </div>
              <div style={{display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(140px,1fr))', gap:'12px'}}>
                <div style={{
                  padding:'14px',
                  border:'1px solid rgba(255, 255, 255, 0.08)',
                  borderRadius:'10px',
                  background:'rgba(17, 17, 23, 0.6)'
                }}>
                  <div style={{fontSize:'12px', color:'var(--muted)', marginBottom:'6px'}}>Account Type</div>
                  <div style={{fontWeight:600, marginTop:'4px', color:'var(--text)'}}>{settingsData?.is_admin ? 'Admin' : 'Regular'}</div>
                </div>
                {settingsData?.created_at && (
                  <div style={{
                    padding:'14px',
                    border:'1px solid rgba(255, 255, 255, 0.08)',
                    borderRadius:'10px',
                    background:'rgba(17, 17, 23, 0.6)'
                  }}>
                    <div style={{fontSize:'12px', color:'var(--muted)', marginBottom:'6px'}}>Member Since</div>
                    <div style={{fontWeight:600, marginTop:'4px', color:'var(--text)'}}>
                      {new Date(settingsData.created_at).toLocaleDateString('en-US', { month: 'short', year: 'numeric' })}
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Delete Account Card */}
            <div className="card" style={{
              padding:'24px',
              background:'rgba(198, 40, 40, 0.08)',
              border:'1px solid rgba(244, 67, 54, 0.2)',
              borderRadius:'16px'
            }}>
              <h3 style={{margin:0, marginBottom:'8px', fontSize:'18px', fontWeight:700, color:'#ef5350'}}>Delete Account</h3>
              <p style={{color:'var(--muted)', marginBottom:'20px', fontSize:'14px'}}>
                Permanently delete your account and all associated data. This action cannot be undone.
              </p>

              {deleteAccountError && (
                <div style={{
                  background:'rgba(198, 40, 40, 0.15)',
                  border:'1px solid rgba(244, 67, 54, 0.3)',
                  color:'#ef5350',
                  padding:'14px',
                  borderRadius:'8px',
                  marginBottom:'16px'
                }}>
                  {deleteAccountError}
                </div>
              )}

              {!showDeleteConfirm ? (
                <button
                  type="button"
                  onClick={() => setShowDeleteConfirm(true)}
                  style={{
                    background:'rgba(244, 67, 54, 0.1)',
                    border:'1px solid rgba(244, 67, 54, 0.3)',
                    color:'#ef5350',
                    padding:'12px 24px',
                    borderRadius:'8px',
                    cursor:'pointer',
                    fontWeight:600,
                    fontSize:'14px',
                    transition:'all 0.2s ease',
                    width:'100%'
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.background = 'rgba(244, 67, 54, 0.2)'
                    e.currentTarget.style.borderColor = 'rgba(244, 67, 54, 0.5)'
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.background = 'rgba(244, 67, 54, 0.1)'
                    e.currentTarget.style.borderColor = 'rgba(244, 67, 54, 0.3)'
                  }}
                >
                  Delete My Account
                </button>
              ) : (
                <div>
                  <div style={{marginBottom:'16px'}}>
                    <label className="form-label" style={{marginBottom:'8px', display:'block'}}>
                      Enter your password to confirm
                    </label>
                    <input
                      type="password"
                      className="form-input"
                      value={deleteAccountPassword}
                      onChange={(e) => setDeleteAccountPassword(e.target.value)}
                      placeholder="Your password"
                      style={{width:'100%'}}
                    />
                  </div>
                  <div style={{marginBottom:'16px'}}>
                    <label className="form-label" style={{marginBottom:'8px', display:'block'}}>
                      Type "DELETE" to confirm
                    </label>
                    <input
                      type="text"
                      className="form-input"
                      value={deleteAccountConfirm}
                      onChange={(e) => setDeleteAccountConfirm(e.target.value)}
                      placeholder="Type DELETE"
                      style={{width:'100%'}}
                    />
                  </div>
                  <div style={{display:'flex', gap:'12px'}}>
                    <button
                      type="button"
                      onClick={async () => {
                        setDeleteAccountError(null)
                        if(!deleteAccountPassword){
                          setDeleteAccountError('Password is required')
                          return
                        }
                        if(deleteAccountConfirm !== 'DELETE'){
                          setDeleteAccountError('Please type "DELETE" to confirm')
                          return
                        }
                        setDeleteAccountDeleting(true)
                        try {
                          const result = await deleteUserAccount(deleteAccountPassword)
                          if(result.ok){
                            // Clear auth and redirect to login
                            setCurrentUser(null)
                            clearAuthToken()
                            try {
                              sessionStorage.removeItem('currentUser')
                              localStorage.removeItem('currentUser')
                              localStorage.removeItem('rememberUser')
                            } catch {}
                            navigateToView('login')
                          } else {
                            setDeleteAccountError(result.error || 'Failed to delete account')
                          }
                        } catch (e: any) {
                          setDeleteAccountError(e?.message || 'An unexpected error occurred')
                        } finally {
                          setDeleteAccountDeleting(false)
                        }
                      }}
                      disabled={deleteAccountDeleting}
                      style={{
                        background:'#ef5350',
                        border:'1px solid #ef5350',
                        color:'#fff',
                        padding:'12px 24px',
                        borderRadius:'8px',
                        cursor:deleteAccountDeleting ? 'not-allowed' : 'pointer',
                        fontWeight:600,
                        fontSize:'14px',
                        opacity:deleteAccountDeleting ? 0.6 : 1,
                        transition:'all 0.2s ease',
                        flex:1
                      }}
                      onMouseEnter={(e) => {
                        if(!deleteAccountDeleting){
                          e.currentTarget.style.background = '#d32f2f'
                          e.currentTarget.style.borderColor = '#d32f2f'
                        }
                      }}
                      onMouseLeave={(e) => {
                        if(!deleteAccountDeleting){
                          e.currentTarget.style.background = '#ef5350'
                          e.currentTarget.style.borderColor = '#ef5350'
                        }
                      }}
                    >
                      {deleteAccountDeleting ? 'Deleting...' : 'Confirm Deletion'}
                    </button>
                    <button
                      type="button"
                      onClick={() => {
                        setShowDeleteConfirm(false)
                        setDeleteAccountPassword('')
                        setDeleteAccountConfirm('')
                        setDeleteAccountError(null)
                      }}
                      disabled={deleteAccountDeleting}
                      style={{
                        background:'rgba(255, 255, 255, 0.05)',
                        border:'1px solid rgba(255, 255, 255, 0.1)',
                        color:'var(--text)',
                        padding:'12px 24px',
                        borderRadius:'8px',
                        cursor:deleteAccountDeleting ? 'not-allowed' : 'pointer',
                        fontWeight:600,
                        fontSize:'14px',
                        opacity:deleteAccountDeleting ? 0.6 : 1,
                        transition:'all 0.2s ease',
                        flex:1
                      }}
                      onMouseEnter={(e) => {
                        if(!deleteAccountDeleting){
                          e.currentTarget.style.background = 'rgba(255, 255, 255, 0.1)'
                        }
                      }}
                      onMouseLeave={(e) => {
                        if(!deleteAccountDeleting){
                          e.currentTarget.style.background = 'rgba(255, 255, 255, 0.05)'
                        }
                      }}
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>

          <div className="card" style={{
            padding:'24px',
            background:'rgba(20, 20, 28, 0.95)',
            border:'1px solid rgba(255, 255, 255, 0.08)',
            borderRadius:'16px'
          }}>
            <h2 className="form-title" style={{marginBottom:'8px', color:'var(--text)'}}>Account Settings</h2>
            <p style={{color:'var(--muted)', marginBottom:'20px', fontSize:'14px'}}>Update your display name, email, or password. Current password is required.</p>

            {settingsSuccess && (
              <div style={{
                background:'rgba(46, 125, 50, 0.15)',
                border:'1px solid rgba(76, 175, 80, 0.3)',
                color:'#81c784',
                padding:'14px',
                borderRadius:'8px',
                marginBottom:'16px',
                display:'flex',
                alignItems:'center',
                gap:'8px'
              }}>
                <span>✓</span>
                <span>{settingsSuccess}</span>
              </div>
            )}

            {settingsError && (
              <div style={{
                background:'rgba(198, 40, 40, 0.15)',
                border:'1px solid rgba(244, 67, 54, 0.3)',
                color:'#ef5350',
                padding:'14px',
                borderRadius:'8px',
                marginBottom:'16px',
                display:'flex',
                alignItems:'center',
                gap:'8px'
              }}>
                <span>⚠️</span>
                <span>{settingsError}</span>
              </div>
            )}

            <form className="auth-form" onSubmit={handleSettingsSubmit}>
              <div className="form-group">
                <label className="form-label">Current Password *</label>
                <input 
                  className="form-input" 
                  type="password" 
                  placeholder="Enter your current password" 
                  value={settingsCurrentPassword} 
                  onChange={e => setSettingsCurrentPassword(e.target.value)}
                  required
                  autoComplete="current-password"
                />
              </div>

              <div className="form-group">
                <label className="form-label">Display Name</label>
                <input 
                  className="form-input" 
                  type="text" 
                  placeholder={settingsData?.display_name || settingsData?.email?.split('@')[0] || 'Enter display name'}
                  value={settingsDisplayName} 
                  onChange={e => setSettingsDisplayName(e.target.value)}
                  maxLength={50}
                  autoComplete="name"
                />
                <div style={{fontSize:'12px', color:'var(--muted)', marginTop:'6px'}}>
                  This is how your name appears throughout the app. Max 50 characters.
                </div>
              </div>

              <div className="form-group">
                <label className="form-label">New Email</label>
                <input 
                  className="form-input" 
                  type="email" 
                  placeholder={settingsData?.email || 'Enter new email'}
                  value={settingsNewEmail} 
                  onChange={e => setSettingsNewEmail(e.target.value)}
                  autoComplete="email"
                />
                <div style={{fontSize:'12px', color:'var(--muted)', marginTop:'6px'}}>
                  Leave blank to keep your current email.
                </div>
              </div>

              <div className="form-group">
                <label className="form-label">New Password</label>
                <input 
                  className="form-input" 
                  type="password" 
                  placeholder="Enter a new password (optional)" 
                  value={settingsNewPassword} 
                  onChange={e => setSettingsNewPassword(e.target.value)}
                  autoComplete="new-password"
                />
                {settingsNewPassword && (
                  <div style={{marginTop:'8px'}}>
                    <div style={{height:'4px', background:'rgba(255, 255, 255, 0.1)', borderRadius:'2px', overflow:'hidden'}}>
                      <div style={{width:`${(passwordStrength.score/4)*100}%`, height:'100%', background:passwordStrength.color, transition:'width 0.2s ease'}}></div>
                    </div>
                    <div style={{fontSize:'12px', color:passwordStrength.color, marginTop:'6px', fontWeight:500}}>
                      {passwordStrength.label}
                    </div>
                  </div>
                )}
              </div>

              <div className="form-group">
                <label className="form-label">Confirm New Password</label>
                <input 
                  className="form-input" 
                  type="password" 
                  placeholder="Re-enter new password" 
                  value={settingsConfirmPassword} 
                  onChange={e => setSettingsConfirmPassword(e.target.value)}
                  disabled={!settingsNewPassword}
                  autoComplete="new-password"
                  style={{background:!settingsNewPassword ? 'rgba(255, 255, 255, 0.03)' : 'transparent'}}
                />
                {settingsNewPassword && settingsConfirmPassword && (
                  <div style={{
                    fontSize:'12px',
                    marginTop:'6px',
                    color: settingsNewPassword === settingsConfirmPassword ? '#81c784' : '#ef5350',
                    fontWeight:500
                  }}>
                    {settingsNewPassword === settingsConfirmPassword ? '✓ Passwords match' : '✗ Passwords do not match'}
                  </div>
                )}
              </div>

              <div style={{display:'flex', gap:'12px', marginTop:'16px'}}>
                <button 
                  className="btn-primary" 
                  type="submit" 
                  disabled={settingsSaving}
                  style={{flex:1}}
                >
                  {settingsSaving ? 'Saving...' : 'Save Changes'}
                </button>
                <button 
                  className="btn-secondary" 
                  type="button" 
                  onClick={() => navigateToView('app')}
                  style={{flex:1}}
                >
                  Cancel
                </button>
              </div>
            </form>
          </div>
        </section>
      </div>
    )
  }

  if(view === 'add'){
    // Admin-only view - redirect non-admins
    if(!currentUser?.is_admin) {
      return (
        <div className="container auth-container">
          <h1 className="form-title">Access Denied</h1>
          <p style={{textAlign:'center', color:'#666', marginBottom:'20px'}}>
            Admin privileges are required to add movies and TV shows.
          </p>
          <button className="btn-primary" onClick={() => navigateToTab('home')}>
            Go to Home
          </button>
        </div>
      )
    }
    
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
                PlotSignal
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
              <h2>Add New {addMediaType === 'movie' ? 'Movie' : 'TV Show'}</h2>
              <p>Expand your catalog with a new title. Fill in the details below to add it to your collection.</p>
            </div>
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
              <div className="poster-score-label">TMDb Score</div>
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
                Movie
              </button>
              <button
                type="button"
                className={addMediaType === 'tv' ? 'active' : ''}
                onClick={()=> setAddMediaType('tv')}
              >
                TV Show
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
                  <label className="form-label">Year</label>
                  <input
                    className="form-input"
                    value={addYear}
                    onChange={e=>setAddYear(e.target.value)}
                    placeholder="e.g. 2024"
                  />
                </div>
                <div className="form-group">
                  <label className="form-label">Language</label>
                  <input
                    className="form-input"
                    value={addLanguage}
                    onChange={e=>setAddLanguage(e.target.value)}
                    placeholder="e.g. en"
                  />
                </div>
                <div className="form-group">
                  <label className="form-label">Popularity</label>
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
                Classification
              </div>
              <div className="form-group">
                <label className="form-label">Genre *</label>
                <input
                  className="form-input"
                  value={addGenre}
                  onChange={e=>setAddGenre(e.target.value)}
                  placeholder="e.g. Drama, Action, Comedy"
                  required
                />
                <span className="genre-hint">Separate multiple genres with commas</span>
              </div>
            </div>

            {/* Media Section */}
            <div className="add-media-section">
              <div className="add-media-section-title">
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
                {addSubmitting ? 'Saving…' : 'Add to Catalog'}
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
                PlotSignal
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
                      <div style={{display:'flex', gap:'12px', alignItems:'center'}}>
                        {currentUser && (
                          <button
                            type="button"
                            className={`detail-watchlist-button ${isInWatchlist ? 'detail-watchlist-button-active' : ''}`}
                            onClick={async () => {
                              if(!currentUser || !detailData) return
                              
                              // Get user_id if not available
                              let userId = currentUser.user_id
                              if(!userId && currentUser.email) {
                                try {
                                  const userData = await getUserByEmail(currentUser.email)
                                  if(userData.ok && userData.user_id) {
                                    userId = userData.user_id
                                    const updatedUser = { ...currentUser, user_id: userId }
                                    setCurrentUser(updatedUser)
                                    try {
                                      sessionStorage.setItem('currentUser', JSON.stringify(updatedUser))
                                      if(localStorage.getItem('rememberUser') === '1') {
                                        localStorage.setItem('currentUser', JSON.stringify(updatedUser))
                                      }
                                    } catch {}
                                  } else {
                                    alert('Please log in to manage your watchlist.')
                                    return
                                  }
                                } catch (err: any) {
                                  alert(`Error: ${err?.message || 'Unknown error'}`)
                                  return
                                }
                              }
                              
                              if(!userId) {
                                alert('Please log in to manage your watchlist.')
                                return
                              }
                              
                              setWatchlistLoading(true)
                              try {
                                const mediaType = detailData.media_type
                                const targetType = mediaType === 'movie' ? 'movie' : 'show'
                                const id = mediaType === 'movie' 
                                  ? (detailData as MovieDetail).movie_id 
                                  : (detailData as ShowDetail).show_id
                                
                                if(isInWatchlist) {
                                  // Remove from watchlist
                                  const result = await removeFromWatchlist(userId, targetType, id)
                                  if(result.ok) {
                                    setIsInWatchlist(false)
                                    // Refresh profile data to update watchlist
                                    if(profileData) {
                                      const updatedProfile = { ...profileData }
                                      if(mediaType === 'movie') {
                                        updatedProfile.watchlist.movies = updatedProfile.watchlist.movies.filter(m => m.id !== id)
                                      } else {
                                        updatedProfile.watchlist.tv = updatedProfile.watchlist.tv.filter(t => t.id !== id)
                                      }
                                      setProfileData(updatedProfile)
                                    }
                                  } else {
                                    alert(`Failed to remove from watchlist: ${result.error}`)
                                  }
                                } else {
                                  // Add to watchlist
                                  const result = await addToWatchlist(userId, targetType, id)
                                  if(result.ok) {
                                    setIsInWatchlist(true)
                                    // Refresh profile data to update watchlist
                                    if(profileData) {
                                      const updatedProfile = { ...profileData }
                                      const item = {
                                        title: detailData.title,
                                        media_type: mediaType,
                                        id: id,
                                        poster_path: detailData.poster_path || null
                                      }
                                      if(mediaType === 'movie') {
                                        updatedProfile.watchlist.movies = [...updatedProfile.watchlist.movies, item]
                                      } else {
                                        updatedProfile.watchlist.tv = [...updatedProfile.watchlist.tv, item]
                                      }
                                      setProfileData(updatedProfile)
                                    }
                                  } else {
                                    alert(`Failed to add to watchlist: ${result.error}`)
                                  }
                                }
                              } catch (err: any) {
                                alert(`Error: ${err?.message || 'Unknown error'}`)
                              } finally {
                                setWatchlistLoading(false)
                              }
                            }}
                            disabled={watchlistLoading || detailLoading || !currentUser}
                            style={{
                              padding:'10px 20px',
                              borderRadius:'8px',
                              border:'1px solid rgba(255, 255, 255, 0.2)',
                              background: isInWatchlist 
                                ? 'rgba(59, 130, 246, 0.95)' 
                                : 'rgba(20, 20, 28, 0.95)',
                              color: isInWatchlist ? '#ffffff' : '#ffffff',
                              cursor: watchlistLoading || detailLoading || !currentUser ? 'not-allowed' : 'pointer',
                              fontWeight:600,
                              fontSize:'14px',
                              transition:'all 0.2s ease',
                              opacity: watchlistLoading || detailLoading || !currentUser ? 0.6 : 1,
                              boxShadow: '0 4px 12px rgba(0, 0, 0, 0.4), 0 0 0 1px rgba(255, 255, 255, 0.1)',
                              backdropFilter: 'blur(8px)',
                              WebkitBackdropFilter: 'blur(8px)'
                            }}
                          >
                            {watchlistLoading ? '...' : (isInWatchlist ? '✓ In Watchlist' : '+ Add to Watchlist')}
                          </button>
                        )}
                        {currentUser?.is_admin && (
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
                            {detailEditMode ? (editSaving ? 'Saving...' : 'Save') : 'Edit'}
                          </button>
                        )}
                      </div>
                    </div>
                  </div>

                  {/* Meta Info Row */}
                  {detailEditMode ? (
                    <div className="detail-edit-form">
                      <div className="detail-edit-header">
                        <span className="detail-edit-header-text">Edit {detailData.media_type === 'movie' ? 'Movie' : 'TV Show'} Details</span>
                      </div>
                      <div className="detail-edit-grid">
                        <div className="detail-edit-row">
                          <label>Year</label>
                          <input
                            type="number"
                            value={editYear}
                            onChange={(e) => setEditYear(e.target.value)}
                            className="detail-edit-input"
                            placeholder="e.g. 2024"
                          />
                        </div>
                        <div className="detail-edit-row">
                          <label>Language</label>
                          <input
                            type="text"
                            value={editLanguage}
                            onChange={(e) => setEditLanguage(e.target.value)}
                            className="detail-edit-input"
                            placeholder="e.g. en"
                          />
                        </div>
                        <div className="detail-edit-row">
                          <label>TMDb Score</label>
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
                          <label>Popularity</label>
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
                          <label>Genre</label>
                          <input
                            type="text"
                            value={editGenre}
                            onChange={(e) => setEditGenre(e.target.value)}
                            className="detail-edit-input"
                            placeholder="e.g. Action, Drama, Comedy"
                          />
                          <span className="detail-edit-hint">Separate multiple genres with commas</span>
                        </div>
                        <div className="detail-edit-row detail-edit-row-full">
                          <label>Synopsis</label>
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
              PlotSignal
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
          <h1>PlotSignal</h1>
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
              {currentUser?.is_admin && (
                <>
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
                </>
              )}
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
              {currentUser?.is_admin && (
                <>
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
                </>
              )}
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
