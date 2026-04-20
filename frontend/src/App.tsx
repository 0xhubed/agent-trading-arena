import { Routes, Route, useLocation } from 'react-router-dom';
import { AnimatePresence, motion } from 'framer-motion';
import { useWebSocket } from './hooks/useWebSocket';
import Dashboard from './components/Dashboard';
import AgentDetail from './components/AgentDetail';
import ErrorBoundary from './components/ErrorBoundary';
import { pageTransition } from './utils/animations';

function App() {
  const location = useLocation();

  // Initialize WebSocket connection
  useWebSocket();

  return (
    <ErrorBoundary>
      <div className="min-h-screen bg-background">
        <AnimatePresence mode="wait">
          <Routes location={location} key={location.pathname}>
            <Route
              path="/"
              element={
                <motion.div
                  variants={pageTransition}
                  initial="initial"
                  animate="animate"
                  exit="exit"
                >
                  <ErrorBoundary>
                    <Dashboard />
                  </ErrorBoundary>
                </motion.div>
              }
            />
            <Route
              path="/agent/:agentId"
              element={
                <motion.div
                  variants={pageTransition}
                  initial="initial"
                  animate="animate"
                  exit="exit"
                >
                  <ErrorBoundary>
                    <AgentDetail />
                  </ErrorBoundary>
                </motion.div>
              }
            />
          </Routes>
        </AnimatePresence>
      </div>
    </ErrorBoundary>
  );
}

export default App;
