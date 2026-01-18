import { Routes, Route, useLocation } from 'react-router-dom';
import { AnimatePresence, motion } from 'framer-motion';
import { useWebSocket } from './hooks/useWebSocket';
import Dashboard from './components/Dashboard';
import AgentDetail from './components/AgentDetail';
import { pageTransition } from './utils/animations';

function App() {
  const location = useLocation();

  // Initialize WebSocket connection
  useWebSocket();

  return (
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
                <Dashboard />
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
                <AgentDetail />
              </motion.div>
            }
          />
        </Routes>
      </AnimatePresence>
    </div>
  );
}

export default App;
